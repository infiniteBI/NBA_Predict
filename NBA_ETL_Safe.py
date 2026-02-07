"""
NBA ETL Pipeline (Hardened Version)
- Retry + exponential backoff
- Safe rate limits
- Idempotent S3 writes
- Graceful failure handling
"""

import boto3
import json
import time
import random
import logging
import os
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta, UTC
from typing import Dict, Any, Optional, List

import pandas as pd
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout

from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv3,
    boxscoreadvancedv3,
    leaguestandingsv3
)
from nba_api.stats.static import teams, players as static_players

# ========================
# ENV + LOGGING
# ========================

env_path = Path(__file__).parent / 'rdsAuthenticator.env'
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========================
# HELPERS
# ========================

def nba_call_with_retry(fn, retries=3, base_delay=2):
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except ReadTimeout:
            if attempt == retries:
                raise
            sleep_time = base_delay * attempt + random.uniform(0.5, 1.5)
            logger.warning(
                f"NBA API timeout (attempt {attempt}/{retries}), retrying in {sleep_time:.1f}s"
            )
            time.sleep(sleep_time)

# ========================
# ETL CLASS
# ========================

class NBAETLPipeline:

    def __init__(self):
        self.bucket = os.getenv("S3_BUCKET_NAME")
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )

    # ---------- S3 ----------
    def s3_exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except:
            return False

    def upload_parquet(self, df: pd.DataFrame, key: str):
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        buffer.seek(0)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )

        logger.info(f"Uploaded: s3://{self.bucket}/{key}")

    def rate_limit(self, seconds=1.2):
        time.sleep(seconds)

    # ---------- DIMENSIONS ----------
    def load_teams(self):
        df = pd.DataFrame(teams.get_teams())
        self.upload_parquet(df, "teams/teams.parquet")

    def load_players(self):
        df = pd.DataFrame(static_players.get_active_players())
        self.upload_parquet(df, "players/players.parquet")

    # ---------- GAMES ----------
    def extract_games(self, season: str, date_from=None, date_to=None) -> pd.DataFrame:
        gf = nba_call_with_retry(
            lambda: leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable="Regular Season",
                league_id_nullable="00"
            )
        )
        df = gf.get_data_frames()[0]
        self.rate_limit()

        if date_from:
            df = df[df["GAME_DATE"] >= date_from]
        if date_to:
            df = df[df["GAME_DATE"] <= date_to]

        return df

    def load_games(self, games_df: pd.DataFrame, season: str):

        games_df["GAME_DATE"] = pd.to_datetime(games_df["GAME_DATE"])

        for game_date, df_day in games_df.groupby(games_df["GAME_DATE"].dt.date):

            rows = []
            for _, r in df_day.iterrows():
                rows.append({
                    "game_id": r["GAME_ID"],
                    "game_date": r["GAME_DATE"],
                    "season": season,
                    "team_id": r["TEAM_ID"],
                    "pts": r["PTS"]
                })

            day_df = pd.DataFrame(rows)

            s3_key = (
                f"games/"
                f"season={season}/"
                f"game_date={game_date}/"
                f"data.parquet"
            )

            if self.s3_exists(s3_key):
                logger.info(f"Skipping games for {game_date} (exists)")
                continue

            self.upload_parquet(day_df, s3_key)


    # ---------- TEAM GAME STATS ----------
    def load_team_game_stats(self, game_id: str, season: str):

        s3_key = f"team_game_stats/season={season}/game_id={game_id}/data.parquet"
        if self.s3_exists(s3_key):
            logger.info(f"Skipping team stats (exists): {game_id}")
            return

        trad = nba_call_with_retry(
            lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        ).get_data_frames()[1]

        self.rate_limit(1.2)

        adv = nba_call_with_retry(
            lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
        ).get_data_frames()[1]

        df = trad.merge(
            adv,
            on=["GAME_ID", "TEAM_ID"],
            how="left",
            suffixes=("", "_adv")
        )

        df["season"] = season
        self.upload_parquet(df, s3_key)

    # ---------- PLAYER GAME STATS ----------
    def load_player_game_stats(self, game_id: str, season: str):

        s3_key = f"player_game_stats/season={season}/game_id={game_id}/data.parquet"
        if self.s3_exists(s3_key):
            logger.info(f"Skipping player stats (exists): {game_id}")
            return

        trad = nba_call_with_retry(
            lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        ).get_data_frames()[0]

        self.rate_limit(1.2)

        adv = nba_call_with_retry(
            lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
        ).get_data_frames()[0]

        df = trad.merge(
            adv[["PLAYER_ID", "USG_PCT"]],
            on="PLAYER_ID",
            how="left"
        )

        df["season"] = season
        self.upload_parquet(df, s3_key)

    # ---------- STANDINGS ----------
    def load_standings(self, season: str):
        standings = nba_call_with_retry(
            lambda: leaguestandingsv3.LeagueStandingsV3(
                league_id="00",
                season=season,
                season_type="Regular Season"
            )
        )

        df = standings.get_data_frames()[0]
        df["snapshot_date"] = datetime.now(UTC).date()
        df["season"] = season

        key = f"standings/season={season}/snapshot_date={df['snapshot_date'][0]}/data.parquet"
        self.upload_parquet(df, key)

    def load_player_team_history(self, game_id: str, season: str):

        today = datetime.utcnow().date()

        s3_key = (
            f"player_team_history/"
            f"season={season}/"
            f"snapshot_date={today}/"
            f"data.parquet"
        )

        # Only one snapshot per day
        if self.s3_exists(s3_key):
            return

        boxscore = nba_call_with_retry(
            lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        )

        players_df = boxscore.get_data_frames()[0]

        history_df = players_df[[
            "PLAYER_ID",
            "TEAM_ID",
            "TEAM_ABBREVIATION"
        ]].drop_duplicates()

        history_df["snapshot_date"] = today
        history_df["season"] = season

        self.upload_parquet(history_df, s3_key)

    def extract_and_load_shot_zones(self, game_id: str, team_id: int, player_id: int, season: str):
        """Extract and load shot zone data for a player in a game"""
        try:
            shot_chart = shotchartdetail.ShotChartDetail(
                team_id=team_id,
                player_id=player_id,
                game_id_nullable=game_id,
                context_measure_simple='FGA',
                season_nullable=season,
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
            zone_stats['game_id'] = game_id
            zone_stats['team_id'] = team_id
            zone_stats['player_id'] = player_id
            zone_stats['season'] = season
            
            # Upload to S3 as Parquet
            s3_key = f'shot_zones/season={season}/game_id={game_id}/player_id={player_id}/data.parquet'
            self.upload_to_s3(zone_stats, s3_key, 'parquet')
            
        except Exception as e:
            logger.error(f"Error processing shot zones for player {player_id} in game {game_id}: {e}")


    # ---------- ORCHESTRATION ----------
    def run(self, season: str, date_from=None, date_to=None, include_shot_zones=False):

        logger.info("Starting NBA ETL")

        self.load_teams()
        self.load_players()

        games = self.extract_games(season, date_from, date_to)
        self.load_games(games, season)

        game_ids = games["GAME_ID"].unique()

        for i, game_id in enumerate(game_ids, 1):
            logger.info(f"[{i}/{len(game_ids)}] Processing game {game_id}")
            try:
                self.load_team_game_stats(game_id, season)
                self.load_player_game_stats(game_id, season)
                self.load_player_team_history(game_id, season)
                if include_shot_zones:
                    boxscore = nba_call_with_retry(
                        lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
                    )
                    players_df = boxscore.get_data_frames()[0]
                    
                    for _, row in players_df.iterrows():
                        if row['PLAYER_ID'] and row.get('MIN', '0:00') != '0:00':
                            self.load_shot_zones(
                                game_id, 
                                row['TEAM_ID'], 
                                row['PLAYER_ID'], 
                                season
                            )
            except Exception as e:
                logger.error(f"Skipping game {game_id}: {e}")

        self.load_standings(season)
        logger.info("ETL COMPLETE")



# ========================
# RUN
# ========================

if __name__ == "__main__":

    pipeline = NBAETLPipeline()

    pipeline.run(
        season="2025-26",
        date_from="2025-10-01",
        date_to="2026-01-01"
    )
