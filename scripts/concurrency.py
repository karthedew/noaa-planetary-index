import json
from kp_demo.concurrency import run_concurrency_demo

if __name__ == "__main__":
    print(json.dumps(run_concurrency_demo(), indent=2))
