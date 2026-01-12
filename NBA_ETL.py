"""
NBA ETL Pipeline - Extract data from NBA API and load to AWS Database
Install required packages: pip install nba_api psycopg2-binary boto3 pandas python-dotenv
"""

import psycopg2
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv2,
    boxscoreadvancedv2,
    boxscorematchups,
    shotchartdetail,
    leaguestandingsv3,
    commonplayerinfo
)
from nba_api.stats.static import teams, players as static_players
from datetime import datetime, timedelta
import time
import pandas as pd
from typing import List, Dict, Any, Optional
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class NBAETLPipeline:
    """ETL Pipeline for NBA data to AWS Database"""
    
    def __init__(self, db_config: Optional[Dict[str, str]] = None):
        """
        Initialize the ETL pipeline
        
        Args:
            db_config: Database connection parameters or None to use environment variables
        """
        if db_config is None:
            db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'database': os.getenv('DB_NAME', 'nba_stats'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', ''),
                'port': int(os.getenv('DB_PORT', 5432))
            }
        
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def rate_limit_delay(self, seconds: float = 0.6):
        """Add delay to respect NBA API rate limits"""
        time.sleep(seconds)
    
    # ========================
    # TEAMS DATA
    # ========================
    
    def extract_teams(self) -> List[Dict[str, Any]]:
        """Extract all NBA teams"""
        logger.info("Extracting teams data...")
        nba_teams = teams.get_teams()
        return nba_teams
    
    def load_teams(self, teams_data: List[Dict[str, Any]]):
        """Load teams into database"""
        logger.info(f"Loading {len(teams_data)} teams...")
        
        insert_query = """
            INSERT INTO teams (team_id, team_name, abbreviation, city, conference, division)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id) DO UPDATE SET
                team_name = EXCLUDED.team_name,
                abbreviation = EXCLUDED.abbreviation,
                city = EXCLUDED.city,
                conference = EXCLUDED.conference,
                division = EXCLUDED.division
        """
        
        data = [
            (
                team['id'],
                team['full_name'],
                team['abbreviation'],
                team['city'],
                team.get('conference', ''),
                team.get('division', '')
            )
            for team in teams_data
        ]
        
        execute_batch(self.cursor, insert_query, data)
        self.conn.commit()
        logger.info("Teams loaded successfully")
    
    # ========================
    # PLAYERS DATA
    # ========================
    
    def extract_players(self) -> List[Dict[str, Any]]:
        """Extract all active NBA players"""
        logger.info("Extracting players data...")
        nba_players = static_players.get_active_players()
        return nba_players
    
    def enrich_player_data(self, player_id: int) -> Dict[str, Any]:
        """Get detailed player information from NBA API"""
        try:
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            df = player_info.get_data_frames()[0]
            self.rate_limit_delay()
            
            if len(df) > 0:
                row = df.iloc[0]
                return {
                    'position': row.get('POSITION', None),
                    'height': row.get('HEIGHT', None),
                    'weight': row.get('WEIGHT', None),
                    'birth_date': row.get('BIRTHDATE', None),
                    'country': row.get('COUNTRY', None),
                    'draft_year': row.get('DRAFT_YEAR', None)
                }
        except Exception as e:
            logger.warning(f"Could not enrich player {player_id}: {e}")
        
        return {}
    
    def load_players(self, players_data: List[Dict[str, Any]], enrich: bool = False):
        """
        Load players into database
        
        Args:
            players_data: List of player dictionaries
            enrich: If True, fetch detailed info for each player (slow!)
        """
        logger.info(f"Loading {len(players_data)} players...")
        
        insert_query = """
            INSERT INTO players (
                player_id, first_name, last_name, full_name,
                position, height, weight, birth_date, country, draft_year
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                full_name = EXCLUDED.full_name,
                position = EXCLUDED.position,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                birth_date = EXCLUDED.birth_date,
                country = EXCLUDED.country,
                draft_year = EXCLUDED.draft_year
        """
        
        data = []
        for i, player in enumerate(players_data, 1):
            if enrich:
                logger.info(f"Enriching player {i}/{len(players_data)}: {player['full_name']}")
                enriched = self.enrich_player_data(player['id'])
            else:
                enriched = {}
            
            data.append((
                player['id'],
                player.get('first_name', ''),
                player.get('last_name', ''),
                player['full_name'],
                enriched.get('position', None),
                enriched.get('height', None),
                enriched.get('weight', None),
                enriched.get('birth_date', None),
                enriched.get('country', None),
                enriched.get('draft_year', None)
            ))
        
        execute_batch(self.cursor, insert_query, data)
        self.conn.commit()
        logger.info("Players loaded successfully")
    
    # ========================
    # GAMES DATA
    # ========================
    
    def extract_games(self, season: str = '2024-25', season_type: str = 'Regular Season'):
        """
        Extract games for a specific season
        
        Args:
            season: NBA season (e.g., '2024-25')
            season_type: 'Regular Season', 'Playoffs', or 'Pre Season'
        """
        logger.info(f"Extracting games for {season} {season_type}...")
        
        gamefinder = leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            season_type_nullable=season_type,
            league_id_nullable='00'
        )
        
        games_df = gamefinder.get_data_frames()[0]
        self.rate_limit_delay()
        
        return games_df
    
    def load_games(self, games_df: pd.DataFrame, season: str):
        """Load games into database"""
        logger.info(f"Processing games for loading...")
        
        # Group by game to get both teams' data
        games_dict = {}
        
        for _, row in games_df.iterrows():
            game_id = row['GAME_ID']
            
            if game_id not in games_dict:
                games_dict[game_id] = {
                    'game_id': game_id,
                    'game_date': row['GAME_DATE'],
                    'season': season,
                    'matchup': row['MATCHUP'],
                    'teams': []
                }
            
            games_dict[game_id]['teams'].append({
                'team_id': row['TEAM_ID'],
                'pts': row['PTS'],
                'is_home': '@' not in row['MATCHUP']
            })
        
        insert_query = """
            INSERT INTO games (game_id, game_date, season, home_team_id, away_team_id, 
                             home_score, away_score, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status = EXCLUDED.status
        """
        
        data = []
        for game_id, game_info in games_dict.items():
            if len(game_info['teams']) == 2:
                home_team = next((t for t in game_info['teams'] if t['is_home']), None)
                away_team = next((t for t in game_info['teams'] if not t['is_home']), None)
                
                if home_team and away_team:
                    data.append((
                        game_id,
                        game_info['game_date'],
                        game_info['season'],
                        home_team['team_id'],
                        away_team['team_id'],
                        home_team['pts'],
                        away_team['pts'],
                        'Final'
                    ))
        
        execute_batch(self.cursor, insert_query, data)
        self.conn.commit()
        logger.info(f"Loaded {len(data)} games")
    
    # ========================
    # TEAM GAME STATS
    # ========================
    
    def extract_and_load_team_game_stats(self, game_id: str):
        """Extract and load team stats for a specific game"""
        try:
            # Get traditional box score
            trad_boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            trad_df = trad_boxscore.get_data_frames()[1]  # Team stats
            self.rate_limit_delay()
            
            # Get advanced box score
            adv_boxscore = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            adv_df = adv_boxscore.get_data_frames()[1]  # Team stats
            self.rate_limit_delay()
            
            # Merge dataframes
            merged_df = pd.merge(
                trad_df, adv_df, 
                on=['GAME_ID', 'TEAM_ID'], 
                suffixes=('', '_adv')
            )
            
            # Get home/away info from games table
            self.cursor.execute("""
                SELECT home_team_id, away_team_id 
                FROM games 
                WHERE game_id = %s
            """, (game_id,))
            
            result = self.cursor.fetchone()
            if not result:
                logger.warning(f"Game {game_id} not found in database")
                return
            
            home_team_id, away_team_id = result
            
            insert_query = """
                INSERT INTO team_game_stats (
                    team_id, game_id, is_home, pts, ast, reb, oreb, dreb,
                    stl, blk, tov, pf, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                    ftm, fta, ft_pct, plus_minus, pace, off_rating, def_rating, net_rating
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (team_id, game_id) DO UPDATE SET
                    pts = EXCLUDED.pts,
                    ast = EXCLUDED.ast,
                    reb = EXCLUDED.reb,
                    plus_minus = EXCLUDED.plus_minus,
                    pace = EXCLUDED.pace,
                    off_rating = EXCLUDED.off_rating,
                    def_rating = EXCLUDED.def_rating,
                    net_rating = EXCLUDED.net_rating
            """
            
            data = []
            for _, row in merged_df.iterrows():
                team_id = row['TEAM_ID']
                is_home = team_id == home_team_id
                
                data.append((
                    team_id,
                    game_id,
                    is_home,
                    row['PTS'],
                    row['AST'],
                    row['REB'],
                    row['OREB'],
                    row['DREB'],
                    row['STL'],
                    row['BLK'],
                    row['TO'],
                    row['PF'],
                    row['FGM'],
                    row['FGA'],
                    row['FG_PCT'],
                    row['FG3M'],
                    row['FG3A'],
                    row['FG3_PCT'],
                    row['FTM'],
                    row['FTA'],
                    row['FT_PCT'],
                    row['PLUS_MINUS'],
                    row.get('PACE', None),
                    row.get('OFF_RATING', None),
                    row.get('DEF_RATING', None),
                    row.get('NET_RATING', None)
                ))
            
            execute_batch(self.cursor, insert_query, data)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing team stats for game {game_id}: {e}")
            self.conn.rollback()
    
    # ========================
    # PLAYER GAME STATS
    # ========================
    
    def extract_and_load_player_game_stats(self, game_id: str):
        """Extract and load player stats for a specific game"""
        try:
            # Get traditional box score
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            players_df = boxscore.get_data_frames()[0]  # Player stats
            self.rate_limit_delay()
            
            # Get advanced box score for usage rate
            adv_boxscore = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            adv_df = adv_boxscore.get_data_frames()[0]  # Player advanced stats
            self.rate_limit_delay()
            
            # Merge traditional and advanced stats
            merged_df = pd.merge(
                players_df, 
                adv_df[['PLAYER_ID', 'USG_PCT']], 
                on='PLAYER_ID', 
                how='left'
            )
            
            insert_query = """
                INSERT INTO player_game_stats (
                    game_id, team_id, player_id, starter, minutes, pts, ast, reb,
                    oreb, dreb, stl, blk, tov, pf, fgm, fga, fg_pct, fg3m, fg3a,
                    fg3_pct, ftm, fta, ft_pct, plus_minus, usage_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, player_id) DO UPDATE SET
                    pts = EXCLUDED.pts,
                    minutes = EXCLUDED.minutes,
                    ast = EXCLUDED.ast,
                    reb = EXCLUDED.reb,
                    usage_rate = EXCLUDED.usage_rate
            """
            
            data = []
            for _, row in merged_df.iterrows():
                if row['PLAYER_ID']:  # Skip team totals
                    # Parse minutes (format: MM:SS or M:SS)
                    minutes_str = str(row.get('MIN', '0:00'))
                    try:
                        if ':' in minutes_str:
                            mins, secs = minutes_str.split(':')
                            minutes = float(mins) + float(secs) / 60
                        else:
                            minutes = float(minutes_str) if minutes_str else 0.0
                    except:
                        minutes = 0.0
                    
                    data.append((
                        game_id,
                        row['TEAM_ID'],
                        row['PLAYER_ID'],
                        row['START_POSITION'] != '' if pd.notna(row['START_POSITION']) else False,
                        round(minutes, 2),
                        row['PTS'],
                        row['AST'],
                        row['REB'],
                        row['OREB'],
                        row['DREB'],
                        row['STL'],
                        row['BLK'],
                        row['TO'],
                        row['PF'],
                        row['FGM'],
                        row['FGA'],
                        row['FG_PCT'],
                        row['FG3M'],
                        row['FG3A'],
                        row['FG3_PCT'],
                        row['FTM'],
                        row['FTA'],
                        row['FT_PCT'],
                        row['PLUS_MINUS'],
                        row.get('USG_PCT', None)
                    ))
            
            execute_batch(self.cursor, insert_query, data)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing player stats for game {game_id}: {e}")
            self.conn.rollback()
    
    # ========================
    # SHOT ZONES DATA
    # ========================
    
    def extract_and_load_shot_zones(self, game_id: str, team_id: int, player_id: int):
        """Extract and load shot zone data for a player in a game"""
        try:
            shot_chart = shotchartdetail.ShotChartDetail(
                team_id=team_id,
                player_id=player_id,
                game_id_nullable=game_id,
                context_measure_simple='FGA',
                season_nullable='2024-25',
                season_type_nullable='Regular Season'
            )
            
            shots_df = shot_chart.get_data_frames()[0]
            self.rate_limit_delay(1.0)  # Longer delay for shot chart API
            
            if len(shots_df) == 0:
                return
            
            # Aggregate by shot zones
            zone_stats = shots_df.groupby([
                'SHOT_ZONE_BASIC', 
                'SHOT_ZONE_AREA', 
                'SHOT_ZONE_RANGE'
            ]).agg({
                'SHOT_MADE_FLAG': ['sum', 'count']
            }).reset_index()
            
            zone_stats.columns = ['zone_basic', 'zone_area', 'zone_range', 'fgm', 'fga']
            zone_stats['fg_pct'] = zone_stats['fgm'] / zone_stats['fga']
            
            insert_query = """
                INSERT INTO shot_zones_game (
                    game_id, team_id, player_id, shot_zone_basic, 
                    shot_zone_area, shot_zone_range, fgm, fga, fg_pct
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, team_id, player_id, shot_zone_basic, 
                           shot_zone_area, shot_zone_range) 
                DO UPDATE SET
                    fgm = EXCLUDED.fgm,
                    fga = EXCLUDED.fga,
                    fg_pct = EXCLUDED.fg_pct
            """
            
            data = [
                (
                    game_id,
                    team_id,
                    player_id,
                    row['zone_basic'],
                    row['zone_area'],
                    row['zone_range'],
                    int(row['fgm']),
                    int(row['fga']),
                    round(row['fg_pct'], 3)
                )
                for _, row in zone_stats.iterrows()
            ]
            
            execute_batch(self.cursor, insert_query, data)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing shot zones for player {player_id} in game {game_id}: {e}")
            self.conn.rollback()
    
    # ========================
    # CONFERENCE STANDINGS
    # ========================
    
    def extract_and_load_standings(self, season: str = '2024-25'):
        """Extract and load current conference standings"""
        try:
            logger.info(f"Extracting standings for {season}...")
            
            standings = leaguestandingsv3.LeagueStandingsV3(
                league_id='00',
                season=season,
                season_type='Regular Season'
            )
            
            standings_df = standings.get_data_frames()[0]
            self.rate_limit_delay()
            
            snapshot_date = datetime.now().date()
            
            insert_query = """
                INSERT INTO conference_standings (
                    snapshot_date, season, team_id, conference, division,
                    conference_rank, division_rank, wins, losses, win_pct,
                    games_back, home_record, road_record, streak, last_10
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date, team_id) DO UPDATE SET
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    win_pct = EXCLUDED.win_pct,
                    conference_rank = EXCLUDED.conference_rank,
                    games_back = EXCLUDED.games_back,
                    streak = EXCLUDED.streak,
                    last_10 = EXCLUDED.last_10
            """
            
            data = []
            for _, row in standings_df.iterrows():
                data.append((
                    snapshot_date,
                    season,
                    row['TeamID'],
                    row['Conference'],
                    row.get('Division', ''),
                    row.get('ConferenceRank', None),
                    row.get('DivisionRank', None),
                    row['WINS'],
                    row['LOSSES'],
                    row['WinPCT'],
                    row.get('ConferenceGamesBack', 0),
                    row.get('HOME', ''),
                    row.get('ROAD', ''),
                    row.get('CurrentStreak', ''),
                    row.get('L10', '')
                ))
            
            execute_batch(self.cursor, insert_query, data)
            self.conn.commit()
            logger.info(f"Loaded standings for {len(data)} teams")
            
        except Exception as e:
            logger.error(f"Error loading standings: {e}")
            self.conn.rollback()
    
    # ========================
    # UTILITY METHODS
    # ========================
    
    def get_games_to_process(self, season: str, date_from: str = None, date_to: str = None) -> List[str]:
        """Get list of game IDs to process based on date filters"""
        query = "SELECT DISTINCT game_id FROM games WHERE season = %s"
        params = [season]
        
        if date_from:
            query += " AND game_date >= %s"
            params.append(date_from)
        
        if date_to:
            query += " AND game_date <= %s"
            params.append(date_to)
        
        query += " ORDER BY game_id"
        
        self.cursor.execute(query, params)
        return [row[0] for row in self.cursor.fetchall()]
    
    # ========================
    # MAIN ETL ORCHESTRATION
    # ========================
    
    def run_full_etl(
        self, 
        season: str = '2024-25', 
        date_from: str = None, 
        date_to: str = None,
        include_shot_zones: bool = False,
        enrich_players: bool = False
    ):
        """
        Run complete ETL pipeline
        
        Args:
            season: NBA season (e.g., '2024-25')
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
            include_shot_zones: If True, load shot zone data (SLOW!)
            enrich_players: If True, fetch detailed player info (SLOW!)
        """
        try:
            self.connect()
            
            # 1. Load teams
            logger.info("=== STEP 1: Loading Teams ===")
            teams_data = self.extract_teams()
            self.load_teams(teams_data)
            
            # 2. Load players
            logger.info("=== STEP 2: Loading Players ===")
            players_data = self.extract_players()
            self.load_players(players_data, enrich=enrich_players)
            
            # 3. Load games
            logger.info("=== STEP 3: Loading Games ===")
            games_df = self.extract_games(season)
            
            # Filter by date if provided
            if date_from:
                games_df = games_df[games_df['GAME_DATE'] >= date_from]
            if date_to:
                games_df = games_df[games_df['GAME_DATE'] <= date_to]
            
            self.load_games(games_df, season)
            
            # 4. Load game stats (team and player)
            logger.info("=== STEP 4: Loading Game Stats ===")
            unique_games = games_df['GAME_ID'].unique()
            
            for i, game_id in enumerate(unique_games, 1):
                logger.info(f"Processing game {i}/{len(unique_games)}: {game_id}")
                self.extract_and_load_team_game_stats(game_id)
                self.extract_and_load_player_game_stats(game_id)
            
            # 5. Load shot zones (optional - very slow!)
            if include_shot_zones:
                logger.info("=== STEP 5: Loading Shot Zones ===")
                
                # Get all player-game combinations
                self.cursor.execute("""
                    SELECT DISTINCT game_id, team_id, player_id 
                    FROM player_game_stats 
                    WHERE game_id = ANY(%s)
                    AND minutes > 0
                """, (list(unique_games),))
                
                player_games = self.cursor.fetchall()
                
                for i, (game_id, team_id, player_id) in enumerate(player_games, 1):
                    logger.info(f"Processing shot zones {i}/{len(player_games)}")
                    self.extract_and_load_shot_zones(game_id, team_id, player_id)
            
            # 6. Load standings
            logger.info("=== STEP 6: Loading Standings ===")
            self.extract_and_load_standings(season)
            
            logger.info("=== ETL COMPLETE ===")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()
    
    def run_incremental_update(self, season: str = '2024-25', days_back: int = 1):
        """
        Run incremental update for recent games
        
        Args:
            season: NBA season
            days_back: Number of days to look back
        """
        date_from = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        date_to = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Running incremental update from {date_from} to {date_to}")
        
        self.run_full_etl(
            season=season,
            date_from=date_from,
            date_to=date_to,
            include_shot_zones=False,
            enrich_players=False
        )


# ========================
# USAGE EXAMPLES
# ========================

if __name__ == "__main__":
    """
    Create a .env file with your database credentials:
    
    DB_HOST=your-rds-endpoint.rds.amazonaws.com
    DB_NAME=nba_stats
    DB_USER=your_username
    DB_PASSWORD=your_password
    DB_PORT=5432
    """
    
    # Initialize pipeline (uses .env file)
    pipeline = NBAETLPipeline()
    
    # === OPTION 1: Full season load (SLOW - multiple hours) ===
    # pipeline.run_full_etl(
    #     season='2024-25',
    #     include_shot_zones=False,  # Set to True for shot data (VERY slow)
    #     enrich_players=False       # Set to True for player details (VERY slow)
    # )
    
    # === OPTION 2: Load specific date range ===
    # pipeline.run_full_etl(
    #     season='2024-25',
    #     date_from='2025-01-01',
    #     date_to='2025-01-10'
    # )
    
    # === OPTION 3: Incremental daily update (recommended for automation) ===
    pipeline.run_incremental_update(season='2024-25', days_back=1)
    
    # === OPTION 4: Custom configuration ===
    # custom_db_config = {
    #     'host': 'localhost',
    #     'database': 'nba_dev',
    #     'user': 'postgres',
    #     'password': 'password',
    #     'port': 5432
    # }
    # custom_pipeline = NBAETLPipeline(db_config
