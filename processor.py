import hashlib
from datetime import datetime

class EventProcessor:
    def __init__(self, repository):
        self.repo = repository
        self.rec_src = "kafka_events_oop"

    @staticmethod
    def _generate_hash(val_str):
        if not val_str: return None
        return hashlib.md5(str(val_str).encode('utf-8')).hexdigest()

    def process_event(self, event):
        load_dts = datetime.utcnow().isoformat()

        user_id = event['user_id']
        user_hk = self._generate_hash(user_id)
        event_time = event['event_time']
        event_id = event['event_id']
        event_type = event['event_type']

        self.repo.save_hub_user(user_hk, user_id, load_dts, self.rec_src)

        if event_type == 'PROFILE_UPDATE':
            sub_level = event['subscription_level']
            city = event['city']
            hash_diff = self._generate_hash(f"{sub_level}_{city}")
            self.repo.save_sat_user_profile(user_hk, hash_diff, sub_level, city, event_time)
            print(f"Обработано SCD2: Юзер {user_id}")

        elif event_type == 'LOGIN':
            login_hk = self._generate_hash(f"{user_hk}_{event_id}")
            self.repo.save_t_link_login(login_hk, user_hk, event['platform'], event_time, load_dts, self.rec_src)
            print(f"Обработан LOGIN: Юзер {user_id}")

        elif event_type == 'ACTION':
            content_id = event.get('content_id')
            content_hk = self._generate_hash(content_id)
            action_hk = self._generate_hash(f"{user_hk}_{event_id}")

            self.repo.save_hub_content(content_hk, content_id, load_dts, self.rec_src)
            self.repo.save_t_link_action(action_hk, user_hk, content_hk, event['action_type'], 
                                         event.get('action_value'), event_time, load_dts, self.rec_src)
            print(f"Обработан ACTION: Юзер {user_id}")
        
        elif event_type == 'LOGIN':
            platform_name = event['platform']

            platform_hk = self._generate_hash(platform_name)
            login_hk = self._generate_hash(f"{user_hk}_{event_id}")

            self.repo.save_hub_platform(platform_hk, platform_name, load_dts, self.rec_src)

            self.repo.save_t_link_login(login_hk, user_hk, platform_hk, event_time, load_dts, self.rec_src)

            print(f"[Data Vault] Записан LOGIN: Юзер {user_id} через {platform_name}")
        
        elif event_type == 'PLATFORM_UPDATE':
            platform_name = event['platform']
            version = event['version']
            is_stable = event['is_stable']

            platform_hk = self._generate_hash(platform_name)
            hash_diff = self._generate_hash(f"{version}_{is_stable}")

            self.repo.save_hub_platform(platform_hk, platform_name, load_dts, self.rec_src)

            self.repo.save_sat_platform(platform_hk, hash_diff, version, is_stable, event_time)

            print(f"[SCD2] Платформа {platform_name} обновлена до {version} (Stable: {is_stable})")
