from configparser import ConfigParser
import os
import ast

def load_config(filename, section):
    """
    Load config ưu tiên biến môi trường (cho Docker), sau đó mới đến file (cho Local)
    """
    # 1. Logic tìm file (giữ nguyên)
    if not os.path.exists(filename):
        base_path = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(base_path))
        filename = os.path.join(project_root, filename)

    if not os.path.exists(filename):
        raise FileNotFoundError(f"❌ Config file not found: {filename}")

    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for key, value in params:
            # --- LOGIC MỚI: OVERRIDE BẰNG ENV VAR ---
            # Quy tắc đặt tên Env: DB_HOST, DB_PORT (in hoa)
            env_key = f"{section.upper()}_{key.upper()}" # VD: POSTGRESQL_HOST
            
            # Xử lý mapping tên đặc biệt nếu cần (VD: DB_HOST thay vì POSTGRESQL_HOST)
            if key == 'host': env_val = os.getenv('DB_HOST') or os.getenv(env_key)
            elif key == 'port': env_val = os.getenv('DB_PORT') or os.getenv(env_key)
            else: env_val = os.getenv(env_key)

            final_value = env_val if env_val else value
            # ----------------------------------------

            try:
                config[key] = ast.literal_eval(final_value)
            except (ValueError, SyntaxError):
                if key == 'api_version' and ',' in str(final_value):
                     config[key] = tuple(map(int, str(final_value).split('.')))
                else:
                     config[key] = final_value
    else:
        raise Exception(f"Section {section} not found in {filename}")

    return config

def get_kafka_config(section='producer'):
    config_file = 'config/kafka_config.properties'
    
    # Load common và specific
    common_conf = load_config(config_file, 'common')
    specific_conf = load_config(config_file, section)
    
    final_conf = common_conf.copy()
    final_conf.update(specific_conf)

    # --- LOGIC MỚI: KAFKA ENV OVERRIDE ---
    # Nếu có biến môi trường KAFKA_BOOTSTRAP_SERVER, dùng nó đè lên file config
    env_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVER')
    if env_bootstrap:
        final_conf['bootstrap_servers'] = env_bootstrap
    # -----------------------------
    
    return final_conf