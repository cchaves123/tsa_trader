from datetime import datetime, timedelta


def get_next_sunday(most_recent_cutoff):
    #today = datetime.today()
    days_ahead = 6 - most_recent_cutoff.weekday()  # weekday(): Monday is 0, Sunday is 6
    if days_ahead <= 0:
        days_ahead += 7
    next_sunday = most_recent_cutoff + timedelta(days=days_ahead)
    return next_sunday.replace(hour=0, minute=0, second=0, microsecond=0)

def get_previous_sunday(most_recent_cutoff):
    next_sunday = get_next_sunday(most_recent_cutoff)
    return next_sunday - timedelta(days=7)

#print(get_next_sunday())
