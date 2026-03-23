import random
import time
from datetime import datetime, timezone
import uuid

class EventGenerator:
    def __init__(self):
        self.cities = ['Moscow', 'London', 'New York', 'Tokyo', 'Berlin']
        self.platforms = ['iOS', 'Android', 'Web']
        self.actions = ['click', 'view', 'purchase', 'like']

    def _get_base_event(self, event_type):
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def generate_login(self):
        event = self._get_base_event('LOGIN')
        event.update({"user_id": random.randint(1, 100), "platform": random.choice(self.platforms)})
        return event

    def generate_action(self):
        event = self._get_base_event('ACTION')
        event.update({"user_id": random.randint(1, 100), "content_id": random.randint(1000, 5000), "action_type": random.choice(self.actions), "action_value": str(random.random())})
        return event

    def generate_profile_update(self):
        event = self._get_base_event('PROFILE_UPDATE')
        event.update({"user_id": random.randint(1, 100), "subscription_level": random.choice(['free', 'basic', 'premium']), "city": random.choice(self.cities)})
        return event

    def generate_platform_update(self):
        event = self._get_base_event('PLATFORM_UPDATE')
        event.update({"platform": random.choice(self.platforms), "version": f"{random.randint(14, 17)}.{random.randint(0, 5)}", "is_stable": random.choice([True, False])})
        return event

    def get_random_event(self):
        generators = [self.generate_login, self.generate_action, self.generate_profile_update, self.generate_platform_update]
        return random.choice(generators)()
