#!/usr/bin/env python3
import argparse, requests, sys, json, time, uuid

def post(base, path, payload):
    r = requests.post(f"{base}{path}", json=payload, timeout=30)
    r.raise_for_status()
    if r.headers.get("content-type","").startswith("application/json"):
        return r.json()
    return {"text": r.text}

def main():
    p = argparse.ArgumentParser(description="Terminal chat with your sms-bot (server talks first).")
    p.add_argument("--base", default="https://sms.supamiltai.com", help="API base URL")
    p.add_argument("--msisdn", required=True, help="Phone number to simulate (e.g. 37060000000)")
    p.add_argument("--intro", default="Sveiki, čia įmonė. Domina naujas darbas?", help="First message the bot sends")
    p.add_argument("--userref", default=None, help="Optional userref for the intro message")
    args = p.parse_args()

    userref = args.userref or f"cli-{uuid.uuid4().hex[:8]}"

    # 1) Bot sends first
    print(f"\n[BOT -> {args.msisdn}] {args.intro}")
    try:
        resp = post(args.base, "/send", {"to": args.msisdn, "body": args.intro, "userref": userref})
        print(f"[server] sent id={resp.get('id')}")
    except Exception as e:
        print(f"[error sending intro] {e}")
        sys.exit(1)

    print("\nType your reply and hit Enter. Ctrl+C to quit.\n")
    while True:
        try:
            user_text = input(f"[YOU <- {args.msisdn}] ").strip()
            if not user_text:
                continue
            # 2) Simulate you replying by calling inbound MO webhook
            mo_payload = {"msisdn": args.msisdn, "message": user_text}
            try:
                mo_resp = post(args.base, "/webhooks/mo", mo_payload)
            except requests.HTTPError as he:
                print(f"[server HTTP {he.response.status_code}] {he.response.text}")
                continue

            # We return {"ok":true,"reply":"..."} from /webhooks/mo (Milestone 0)
            bot_reply = mo_resp.get("reply")
            if bot_reply:
                print(f"[BOT -> {args.msisdn}] {bot_reply}")
            else:
                # If classifier branch / fallback
                intent = mo_resp.get("intent")
                print(f"[BOT] (no direct text; intent={intent})")
        except KeyboardInterrupt:
            print("\nBye!")
            break
        except Exception as e:
            print(f"[runtime error] {e}")
            time.sleep(0.5)

if __name__ == "__main__":
    main()
