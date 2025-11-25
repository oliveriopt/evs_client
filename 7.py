def build_connection_string(cfg: dict) -> str:
    driver = cfg.get('driver', 'ODBC Driver 17 for SQL Server')
    server = cfg.get('privateIPaddress') or cfg.get('publicIPaddress') or cfg.get('server')

    # { y } literales se escapan como {{ y }}; {driver} y {server} son expresiones
    parts = [f"DRIVER={{{driver}}};SERVER={server},1433;"]

    if cfg.get('database'):
        parts.append(f"DATABASE={cfg['database']};")
    if cfg.get('username'):
        parts.append(f"UID={cfg['username']};")
    if cfg.get('password'):
        parts.append(f"PWD={cfg['password']};")
    parts.append("Encrypt=yes;TrustServerCertificate=yes;Packet Size=512;")

    conn_str = ''.join(parts)
    print(conn_str)
    return conn_str
