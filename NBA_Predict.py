# NBA_Predict_fixed_schedule_and_recent_games.py
from nba_api.stats.library.parameters import LeagueIDNullable
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, teamgamelog, commonteamroster, playergamelog
import pandas as pd
import time
import json
import requests

# Inject required browser headers for ALL nba_api Stats requests
from nba_api.stats.library.http import NBAStatsHTTP

NBAStatsHTTP.headers = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
AppleWebKit/537.36 (KHTML, like Gecko) \
Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
}


from datetime import datetime
import warnings

warnings.filterwarnings("ignore")


# -------------------------
# Helpers
# -------------------------
def team_info(team_name: str):
    nba_teams = teams.get_teams()
    q = team_name.lower().strip()
    for t in nba_teams:
        if (
            q == t["full_name"].lower()
            or q == t["nickname"].lower()
            or q == t["abbreviation"].lower()
            or q in t["full_name"].lower()
            or q in t["nickname"].lower()
        ):
            return t["id"], t["full_name"], t["abbreviation"]
    return None, None, None


def get_current_season():
    """Return season string like '2025-26' based on current date."""
    now = datetime.now()
    year = now.year
    month = now.month
    if month >= 10:  # Oct-Dec -> new season YEAR-(YEAR+1)
        return f"{year}-{str(year + 1)[-2:]}"
    else:  # Jan-Sep -> previous season (YEAR-1)-YEAR
        return f"{year - 1}-{str(year)[-2:]}"


def safe_get_df(endpoint_obj):
    """Return first dataframe or empty df; swallow errors."""
    try:
        dfs = endpoint_obj.get_data_frames()
        if dfs:
            return dfs[0]
    except Exception as e:
        # keep the message brief so output is readable
        print(f"[API ERROR] {e}")
    return pd.DataFrame()


# -------------------------
# Team schedule / games (FIXED)
# -------------------------
def get_team_schedule(team_id):
    """
    Return full team gamelog DataFrame.
    Try: 1) current season, 2) no season (fallback).
    Normalize GAME_DATE to datetime if possible.
    """
    # Try with current season first
    season = get_current_season()
    try:
        gl = teamgamelog.TeamGameLog(team_id=team_id, season=season)
        df = safe_get_df(gl)
        if df.empty:
            # fallback: try without season filter
            print(f"[get_team_schedule] No schedule for team {team_id} with season={season}; trying no season filter.")
            gl2 = teamgamelog.TeamGameLog(team_id=team_id)
            df = safe_get_df(gl2)
        if df.empty:
            return pd.DataFrame()
        # Normalize game date: try common fields
        if "GAME_DATE" in df.columns:
            df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
        elif "GAME_DATE_EST" in df.columns:
            df["GAME_DATE_EST"] = pd.to_datetime(df["GAME_DATE_EST"], errors="coerce")
            df["GAME_DATE"] = df["GAME_DATE_EST"]
        else:
            # try to infer
            df["GAME_DATE"] = pd.to_datetime(df.get("GAME_DATE", None), errors="coerce")
        return df
    except Exception as e:
        print(f"[get_team_schedule] error: {e}")
        return pd.DataFrame()


def find_next_game(team_a_id, team_b_id, team_a_abbr, team_b_abbr):
    """
    Find the next scheduled game between two teams.
    Uses team A schedule as primary source but checks both schedules as fallback.
    Compares dates using date() (ignores time-of-day).
    """
    today_date = datetime.now().date()

    # Helper to search one schedule
    def search_schedule(df, perspective_abbr, other_abbr):
        if df.empty:
            return None
        # ensure GAME_DATE exists
        if "GAME_DATE" not in df.columns:
            return None
        # sort ascending to find the next
        for _, row in df.sort_values("GAME_DATE").iterrows():
            gdate = row.get("GAME_DATE")
            if pd.isna(gdate):
                continue
            # convert if Timestamp
            gdate_date = pd.to_datetime(gdate).date()
            # matchups are strings, check if opponent abbr present
            matchup = str(row.get("MATCHUP", ""))
            # match if other abbreviation in matchup text
            if other_abbr in matchup and gdate_date >= today_date:
                is_home = "vs." in matchup
                return {
                    "date": gdate_date.strftime("%Y-%m-%d"),
                    "matchup": matchup,
                    "home_team": perspective_abbr if is_home else other_abbr,
                    "away_team": other_abbr if is_home else perspective_abbr,
                }
        return None

    # 1) Try team A schedule
    sched_a = get_team_schedule(team_a_id)
    result = search_schedule(sched_a, team_a_abbr, team_b_abbr)
    if result:
        return result

    # 2) Try team B schedule (in case perspective differs)
    sched_b = get_team_schedule(team_b_id)
    result = search_schedule(sched_b, team_b_abbr, team_a_abbr)
    if result:
        # flip home/away to match A vs B perspective
        # if result's home_team == team_b_abbr then A is away
        return result

    # 3) As a last resort: use LeagueGameFinder search for either team (no season filter)
    try:
        finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_a_id)
        all_games = safe_get_df(finder)
        if not all_games.empty and "GAME_DATE" in all_games.columns:
            all_games["GAME_DATE"] = pd.to_datetime(all_games["GAME_DATE"], errors="coerce")
            for _, row in all_games.sort_values("GAME_DATE").iterrows():
                gdate = row["GAME_DATE"]
                matchup = str(row.get("MATCHUP", ""))
                if team_b_abbr in matchup and pd.to_datetime(gdate).date() >= today_date:
                    is_home = "vs." in matchup
                    return {
                        "date": pd.to_datetime(gdate).date().strftime("%Y-%m-%d"),
                        "matchup": matchup,
                        "home_team": team_a_abbr if is_home else team_b_abbr,
                        "away_team": team_b_abbr if is_home else team_a_abbr,
                    }
    except Exception as e:
        print(f"[find_next_game fallback] {e}")

    # If nothing found
    print("[find_next_game] No upcoming game found in schedules or league finder.")
    return None


# -------------------------
# Last N games & opponent points (FIXED)
# -------------------------
def last_n_games(team_id, n=10):
    """
    Return last n games from teamgamelog (current season or fallback).
    """
    df = get_team_schedule(team_id)
    if df.empty:
        # fallback to leaguegamefinder (which may include more seasons)
        try:
            finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            df = safe_get_df(finder)
            if not df.empty and "GAME_DATE" in df.columns:
                df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
        except Exception as e:
            print(f"[last_n_games fallback] {e}")
            return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df_sorted = df.sort_values("GAME_DATE", ascending=False)
    return df_sorted.head(n)


def get_opponent_points(team_id, n=10):
    """
    Return DataFrame of last n games with TEAM_PTS and OPP_PTS.
    Use LeagueGameFinder if possible; if it doesn't return two rows per game, fallback to teamgamelog+gamefinder per GAME_ID.
    """
    # 1) Try LeagueGameFinder with season first
    season = get_current_season()
    try:
        finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id, season_nullable=season)
        df = safe_get_df(finder)
        if df.empty:
            # fallback to no season
            print(f"[get_opponent_points] no games from LeagueGameFinder for team {team_id} season={season}; trying no season filter.")
            finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            df = safe_get_df(finder)

        if df.empty:
            # fallback: use teamgamelog team schedule and then try to fetch opponent scores using leaguegamefinder per GAME_ID
            print(f"[get_opponent_points] fallback to teamgamelog for team {team_id}")
            schedule = get_team_schedule(team_id)
            if schedule.empty:
                return pd.DataFrame()
            schedule = schedule.sort_values("GAME_DATE", ascending=False).head(n)
            results = []
            for _, row in schedule.iterrows():
                gid = row.get("GAME_ID")
                if not gid:
                    continue
                # fetch leaguegamefinder for this game id (no direct endpoint for single game scoreboard here; reuse finder)
                finder2 = leaguegamefinder.LeagueGameFinder()
                game_rows = safe_get_df(finder2)
                # If game_rows empty, continue
                if game_rows.empty:
                    continue
                game_rows = game_rows[game_rows["GAME_ID"] == gid]
                if game_rows.shape[0] < 2:
                    continue
                team_row = game_rows[game_rows["TEAM_ID"] == team_id].iloc[0]
                opp_row = game_rows[game_rows["TEAM_ID"] != team_id].iloc[0]
                results.append(
                    {
                        "GAME_DATE": row["GAME_DATE"],
                        "MATCHUP": row.get("MATCHUP", ""),
                        "WL": row.get("WL", ""),
                        "TEAM_PTS": int(team_row.get("PTS", 0)),
                        "OPP_PTS": int(opp_row.get("PTS", 0)),
                        "HOME_AWAY": "HOME" if "vs." in str(row.get("MATCHUP", "")) else "AWAY",
                    }
                )
            return pd.DataFrame(results)

        # At this point df from LeagueGameFinder has data. Build results by grouping GAME_ID.
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
        df = df.sort_values("GAME_DATE", ascending=False)
        results = []
        seen = set()
        for _, row in df.iterrows():
            gid = row["GAME_ID"]
            if gid in seen or len(results) >= n:
                continue
            same_game = df[df["GAME_ID"] == gid]
            if same_game.shape[0] < 2:
                # try skip if only one row
                continue
            seen.add(gid)
            # team row = TEAM_ID == team_id
            try:
                team_row = same_game[same_game["TEAM_ID"] == team_id].iloc[0]
                opp_row = same_game[same_game["TEAM_ID"] != team_id].iloc[0]
            except Exception:
                # fallback to first two rows
                team_row = same_game.iloc[0]
                opp_row = same_game.iloc[1]
            results.append(
                {
                    "GAME_DATE": row["GAME_DATE"],
                    "MATCHUP": row.get("MATCHUP", ""),
                    "WL": row.get("WL", ""),
                    "TEAM_PTS": int(team_row.get("PTS", 0)),
                    "OPP_PTS": int(opp_row.get("PTS", 0)),
                    "HOME_AWAY": "HOME" if "vs." in str(row.get("MATCHUP", "")) else "AWAY",
                }
            )
        return pd.DataFrame(results)
    except Exception as e:
        print(f"[get_opponent_points error] {e}")
        return pd.DataFrame()


# -------------------------
# Other utilities (unchanged)
# -------------------------
def get_h2h_detailed(team_a_id, team_b_id, num_games=5):
    try:
        a_finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_a_id, vs_team_id_nullable=team_b_id)
        b_finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_b_id, vs_team_id_nullable=team_a_id)
        games_a = safe_get_df(a_finder)
        games_b = safe_get_df(b_finder)
        if games_a.empty or games_b.empty:
            return pd.DataFrame()
        merged = pd.merge(
            games_a[["GAME_ID", "GAME_DATE", "MATCHUP", "WL", "PTS", "SEASON_ID"]],
            games_b[["GAME_ID", "PTS"]],
            on="GAME_ID",
            suffixes=("_A", "_B"),
        )
        merged["GAME_DATE"] = pd.to_datetime(merged["GAME_DATE"], errors="coerce")
        merged = merged.sort_values("GAME_DATE", ascending=False)
        merged = merged.rename(columns={"WL": "WL_A"})
        cols = ["GAME_DATE", "MATCHUP", "WL_A", "PTS_A", "PTS_B", "SEASON_ID"]
        return merged.head(num_games)[cols]
    except Exception as e:
        print(f"[get_h2h_detailed] {e}")
        return pd.DataFrame()


def get_team_roster(team_id):
    try:
        season = get_current_season()
        r = commonteamroster.CommonTeamRoster(team_id=team_id, season=season)
        df = safe_get_df(r)
        return df
    except Exception as e:
        print(f"[get_team_roster] {e}")
        return pd.DataFrame()


def get_player_last_n_games(player_id, n=10):
    try:
        time.sleep(0.5)
        season = get_current_season()
        gl = playergamelog.PlayerGameLog(player_id=player_id, season=season)
        df = safe_get_df(gl)
        if df.empty:
            # fallback to no-season call
            gl2 = playergamelog.PlayerGameLog(player_id=player_id)
            df = safe_get_df(gl2)
        if df.empty:
            return {}
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
        df = df.sort_values("GAME_DATE", ascending=False).head(n)
        return {
            "PTS": float(df["PTS"].mean()) if "PTS" in df.columns else 0.0,
            "REB": float(df["REB"].mean()) if "REB" in df.columns else 0.0,
            "AST": float(df["AST"].mean()) if "AST" in df.columns else 0.0,
            "BLK": float(df["BLK"].mean()) if "BLK" in df.columns else 0.0,
            "STL": float(df["STL"].mean()) if "STL" in df.columns else 0.0,
            "GAMES": int(len(df)),
        }
    except Exception as e:
        # print(f"[get_player_last_n_games] {e}")
        return {}


def get_player_vs_team(player_id, vs_team_abbr, n=5):
    try:
        time.sleep(0.5)
        gl = playergamelog.PlayerGameLog(player_id=player_id)
        df = safe_get_df(gl)
        if df.empty or "MATCHUP" not in df.columns:
            return {}
        mask = df["MATCHUP"].astype(str).str.contains(str(vs_team_abbr))
        vs_df = df[mask].sort_values("GAME_DATE", ascending=False).head(n)
        if vs_df.empty:
            return {}
        return {
            "PTS": float(vs_df["PTS"].mean()) if "PTS" in vs_df.columns else 0.0,
            "REB": float(vs_df["REB"].mean()) if "REB" in vs_df.columns else 0.0,
            "AST": float(vs_df["AST"].mean()) if "AST" in vs_df.columns else 0.0,
            "BLK": float(vs_df["BLK"].mean()) if "BLK" in vs_df.columns else 0.0,
            "STL": float(vs_df["STL"].mean()) if "STL" in vs_df.columns else 0.0,
            "GAMES": int(len(vs_df)),
        }
    except Exception:
        return {}


# -------------------------
# Main (shortened to show usage)
# -------------------------
def main():
    team_a_input = input("Enter Team A name: ")
    team_b_input = input("Enter Team B name: ")

    team_a_id, team_a_full, team_a_abbr = team_info(team_a_input)
    team_b_id, team_b_full, team_b_abbr = team_info(team_b_input)

    if not team_a_id or not team_b_id:
        print("Invalid team names")
        return

    print(f"Season detected: {get_current_season()}")
    print(f"Looking for next game between {team_a_full} and {team_b_full}...")

    next_game = find_next_game(team_a_id, team_b_id, team_a_abbr, team_b_abbr)
    if next_game:
        print("Next Game Found:", next_game)
    else:
        print("No upcoming game found in schedules (try again or check API availability).")

    print("\nFetching last 10 games for each team (with opponent points):")
    ga = get_opponent_points(team_a_id, 10)
    gb = get_opponent_points(team_b_id, 10)
    if ga.empty:
        print(f"No data available on last 10 games for {team_a_full}")
    else:
        print(ga.to_string(index=False))
    if gb.empty:
        print(f"No data available on last 10 games for {team_b_full}")
    else:
        print(gb.to_string(index=False))


if __name__ == "__main__":
    main()
