"""Spotify listening data scoring and points calculation"""
from dataclasses import dataclass

@dataclass
class PointsBreakdown:
    """Detailed breakdown of points awarded"""
    volume_points: int
    volume_reason: str
    diversity_points: int
    diversity_reason: str
    history_points: int
    history_reason: str
    total_points: int

class ContributionScorer:
    """Calculates points and scores for Spotify listening data contributions"""

    def calculate_score(self, stats) -> PointsBreakdown:
        """Calculate total points based on listening stats"""
        # Calculate points for each category
        volume_points, volume_reason = self.calculate_volume_points(stats.total_minutes)
        diversity_points, diversity_reason = self.calculate_diversity_points(len(stats.unique_artists))
        history_points, history_reason = self.calculate_history_points(stats.activity_period_days)

        # Calculate total points
        total_points = volume_points + diversity_points + history_points

        return PointsBreakdown(
            volume_points=volume_points,
            volume_reason=volume_reason,
            diversity_points=diversity_points,
            diversity_reason=diversity_reason,
            history_points=history_points,
            history_reason=history_reason,
            total_points=total_points
        )

    def calculate_volume_points(self, total_minutes: int) -> tuple[int, str]:
        """
        Calculate points based on total listening time

        Points scale:
        - 5000+ minutes (~3.5 days) = 500 points
        - 1000+ minutes (~17 hours) = 150 points
        - 500+ minutes (~8 hours) = 50 points
        - 100+ minutes (~1.7 hours) = 25 points
        - 30+ minutes = 5 points
        """
        if total_minutes >= 5000:
            return 500, "500 (5000+ minutes)"
        elif total_minutes >= 1000:
            return 150, "150 (1000+ minutes)"
        elif total_minutes >= 500:
            return 50, "50 (500+ minutes)"
        elif total_minutes >= 100:
            return 25, "25 (100+ minutes)"
        elif total_minutes >= 30:
            return 5, "5 (30+ minutes)"
        return 0, "0 (< 30 minutes)"

    def calculate_diversity_points(self, unique_artists: int) -> tuple[int, str]:
        """
        Calculate points based on artist diversity

        Points scale:
        - 50+ artists = 150 points
        - 25+ artists = 75 points
        - 10+ artists = 30 points
        - 5+ artists = 10 points
        - 3+ artists = 5 points
        """
        if unique_artists >= 50:
            return 150, "150 (50+ artists)"
        elif unique_artists >= 25:
            return 75, "75 (25+ artists)"
        elif unique_artists >= 10:
            return 30, "30 (10+ artists)"
        elif unique_artists >= 5:
            return 10, "10 (5+ artists)"
        elif unique_artists >= 3:
            return 5, "5 (3+ artists)"
        return 0, "0 (< 3 artists)"

    def calculate_history_points(self, days: int) -> tuple[int, str]:
        """
        Calculate points based on listening history length

        Points scale:
        - 180+ days (6 months) = 100 points
        - 90+ days (3 months) = 50 points
        - 30+ days (1 month) = 25 points
        - 7+ days = 10 points
        """
        if days >= 180:
            return 100, "100 (6+ months)"
        elif days >= 90:
            return 50, "50 (3+ months)"
        elif days >= 30:
            return 25, "25 (1+ month)"
        elif days >= 7:
            return 10, "10 (7+ days)"
        return 0, "0 (< 7 days)"

    def normalize_score(self, points: int, max_points: int) -> float:
        """Convert points to a normalized score between 0 and 1"""
        if max_points <= 0:
            return 0.0
        return min(1.0, points / max_points)
