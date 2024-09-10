import json
import re
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import ARRAY
from base import Base

class Match(Base):
    __tablename__ = 'matches'
    __table_args__ = {'schema': 'raw'}

    match_id = Column(Integer, primary_key=True)
    match_dates = Column(ARRAY(Date))
    series_name = Column(String)
    match_num = Column(Integer, nullable=True)
    match_stage = Column(String, nullable=True)
    match_type = Column(String)
    match_type_num = Column(Integer, nullable=True)
    season = Column(Integer)
    teams = Column(ARRAY(String))  # Store teams as an array
    venue = Column(String)
    toss_winner = Column(String)
    toss_decision = Column(String)
    outcome_type = Column(String)
    winner = Column(String, nullable=True)
    win_type = Column(String, nullable=True)
    win_by = Column(Integer, nullable=True)

    def __repr__(self):
        return f"<Match(match_id={self.match_id}, series_name={self.series_name}, season={self.season}, venue={self.venue})>"

    @classmethod
    def from_json(cls, file_path):
        """Create a Match object from a JSON file."""
        try:
            with open(file_path, 'r') as file:
                match_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Error reading or parsing {file_path}: {e}")
        
        match_info = match_data.get('info', {})

        match_id = int(re.search(r'(\d+).json', file_path).group(1))  # Ensure match_id is an integer
        match_dates = match_info.get('dates', [])
        series_name = match_info.get('event', {}).get('name', None)
        match_num = match_info.get('event', {}).get('match_number', None)
        match_stage = match_info.get('event', {}).get('stage', None)
        match_type = match_info.get('match_type', None)
        match_type_num = match_info.get('match_type_number', None)
        season = match_info.get('season', None)
        teams = match_info.get('teams', [])
        venue = match_info.get('venue', None)
        toss_winner = match_info.get('toss', {}).get('winner', None)
        toss_decision = match_info.get('toss', {}).get('decision', None)
        outcome_type = match_info.get('outcome', {}).get('result', 'win')
        
        # Handling winner, win_type, and win_by
        winner = None
        win_type = None
        win_by = None
        if outcome_type in ['win', 'tie']:
            winner = match_info.get('outcome', {}).get('winner') or match_info.get('outcome', {}).get('eliminator')
            by_dict = match_info.get('outcome', {}).get('by', None)
            if isinstance(by_dict, dict):
                win_type, win_by = next(iter(by_dict.items()), (None, None))
            if match_info.get('outcome', {}).get('eliminator'):
                win_type = 'super over'

        return cls(
            match_id=match_id,
            match_dates=match_dates,
            series_name=series_name,
            match_num=match_num,
            match_stage=match_stage,
            match_type=match_type,
            match_type_num=match_type_num,
            season=season,
            teams=teams,
            venue=venue,
            toss_winner=toss_winner,
            toss_decision=toss_decision,
            outcome_type=outcome_type,
            winner=winner,
            win_type=win_type,
            win_by=win_by
        )
