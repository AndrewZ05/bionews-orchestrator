import json
import os

from onetrust_probes.common import OneTrustSession, get_env


def main() -> None:
    hostname = get_env("ONETRUST_HOSTNAME")
    client_id = get_env("ONETRUST_CLIENT_ID")
    client_secret = get_env("ONETRUST_CLIENT_SECRET")
    scopes = os.getenv("ONETRUST_SCOPES")

    session = OneTrustSession(hostname, client_id, client_secret, scopes=scopes)
    token = session.get_token()

    output = {
        "token_type": token.token_type,
        "scope": token.scope,
        "expires_at": token.expires_at,
        "base_url": session.base_url,
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
