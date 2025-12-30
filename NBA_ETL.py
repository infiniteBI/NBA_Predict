"""
NBA ETL Pipeline - Extract data from NBA API and load to AWS Database
Install required packages: pip install nba_api psycopg2-binary boto3 pandas
"""

import psycopg2
from psycopg2.extras import execute_batch
from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv2,
    boxscoreadvancedv2,
    shotchartdetail,
    leaguestandingsv3,
    commonteamroster,
    commonplayerinfo
)
from nba_api.stats.static import teams, players as static_players
from datetime import datetime, timedelta
import time
import pandas as pd
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NBAETLPipeline:
    """ETL Pipeline for NBA data to AWS Database"""
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the ETL pipeline
        
        Args:
            db_config: Database connection parameters
                - host: Database host
                - database: Database name
                - user: Database user
                - password: Database password
                - port: Database port (default 5432)
        """
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
    
    def load_players(self, players_data: List[Dict[str, Any]]):
        """Load players into database"""
        logger.info(f"Loading {len(players_data)} players...")
        
        insert_query = """
            INSERT INTO players (player_id, first_name, last_name, full_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                full_name = EXCLUDED.full_name
        """
        
        data = [
            (
                player['id'],
                player.get('first_name', ''),
                player.get('last_name', ''),
                player['full_name']
            )
            for player in players_data
        ]
        
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
        logger.info(f"Loading games...")
        
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
                home_team = next(t for t in game_info['teams'] if t['is_home'])
                away_team = next(t for t in game_info['teams'] if not t['is_home'])
                
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
            merged_df = pd.merge(trad_df, adv_df, on=['GAME_ID', 'TEAM_ID'], suffixes=('', '_adv'))
            
            # Get home/away info from games table
            self.cursor.execute("""
                SELECT home_team_id, away_team_id 
                FROM games 
                WHERE game_id = %s
            """, (game_id,))
            
            result = self.cursor.fetchone()
            if not result:
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
                    plus_minus = EXCLUDED.plus_minus
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
            logger.error(f"Error processing game {game_id}: {e}")
    
    # ========================
    # PLAYER GAME STATS
    # ========================
    
    def extract_and_load_player_game_stats(self, game_id: str):
        """Extract and load player stats for a specific game"""
        try:
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            players_df = boxscore.get_data_frames()[0]  # Player stats
            self.rate_limit_delay()
            
            insert_query = """
                INSERT INTO player_game_stats (
                    game_id, team_id, player_id, starter, minutes, pts, ast, reb,
                    oreb, dreb, stl, blk, tov, pf, fgm, fga, fg_pct, fg3m, fg3a,
                    fg3_pct, ftm, fta, ft_pct, plus_minus
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, team_id, player_id) DO UPDATE SET
                    pts = EXCLUDED.pts,
                    minutes = EXCLUDED.minutes
            """
            
            data = []
            for _, row in players_df.iterrows():
                if row['PLAYER_ID']:  # Skip team totals
                    # Parse minutes (format: MM:SS)
                    minutes_str = str(row.get('MIN', '0:00'))
                    try:
                        mins, secs = minutes_str.split(':')
                        minutes = float(mins) + float(secs) / 60
                    except:
                        minutes = 0.0
                    
                    data.append((
                        game_id,
                        row['TEAM_ID'],
                        row['PLAYER_ID'],
                        row['START_POSITION'] != '',
                        minutes,
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
                        row['PLUS_MINUS']
                    ))
            
            execute_batch(self.cursor, insert_query, data)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing player stats for game {game_id}: {e}")
    
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
                    win_pct = EXCLUDED.win_pct
            """
            
            data = []
            for _, row in standings_df.iterrows():
                data.append((
                    snapshot_date,
                    season,
                    row['TeamID'],
                    row['Conference'],
                    row.get('Division', ''),
                    row['ConferenceRecord'].split('-')[0] if 'ConferenceRecord' in row else None,
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
            logger.info("Standings loaded successfully")
            
        except Exception as e:
            logger.error(f"Error loading standings: {e}")
    
    # ========================
    # MAIN ETL ORCHESTRATION
    # ========================
    
    def run_full_etl(self, season: str = '2024-25', date_from: str = None, date_to: str = None):
        """
        Run complete ETL pipeline
        
        Args:
            season: NBA season (e.g., '2024-25')
            date_from: Start date (YYYY-MM-DD format)
            date_to: End date (YYYY-MM-DD format)
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
            self.load_players(players_data)
            
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
            
            # 5. Load standings
            logger.info("=== STEP 5: Loading Standings ===")
            self.extract_and_load_standings(season)
            
            logger.info("=== ETL COMPLETE ===")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()


# ========================
# USAGE EXAMPLE
# ========================

if __name__ == "__main__":
    # Database configuration for AWS RDS
    db_config = {
        'host': 'your-rds-endpoint.rds.amazonaws.com',
        'database': 'nba_stats',
        'user': 'your_username',
        'password': 'your_password',
        'port': 5432
    }
    
    # Initialize pipeline
    pipeline = NBAETLPipeline(db_config)
    
    # Run full ETL for current season
    # Option 1: Load entire season
    pipeline.run_full_etl(season='2024-25')
    
    # Option 2: Load specific date range
    # pipeline.run_full_etl(
    #     season='2024-25',
    #     date_from='2024-12-01',
    #     date_to='2024-12-31'
    # )
    
    # Option 3: Incremental load (yesterday's games)
    # yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    # pipeline.run_full_etl(
    #     season='2024-25',
    #     date_from=yesterday,
    #     date_to=yesterday
    # )