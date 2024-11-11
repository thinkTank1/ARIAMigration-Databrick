import os
import argparse
from random import randint


"""
    Utility script to populate a .env file based on the current environment configuration

    Note: If you are extending this, ENSURE that no confidential information is added here
"""


def populate_env_file(env: str):
    """
        Utility function to populate a .env file based on the current environment configuration
    """
    data_to_write = {
        # Account name is either dc-purview or dc-mspurview (for test)
        "PURVIEW_ACCOUNT": "dc-" + ("ms" if env != "dev" else "") + f"purview-{env}",
        "ENV": env,
        "DEVOPS_AGENT_NAME": f"DTS Bootstrap (sub:dts-sharedservices-{env})",
        "BUILD_ID": "".join([str(randint(0, 9)) for x in range(0, 5)])
    }
    print("Writing .env file")
    with open(".env", "w") as env_file:
        for k, v in data_to_write.items():
            print(f"    Writing variable {k}")
            env_file.write(f'export {k}="{v}"\n')


if __name__ == '__main__':
    # Fetch kwargs
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", help="Environment to switch to", required=True)
    args = parser.parse_args()
    env = args.env
    if not os.getenv("TF_BUILD"):
        if env == "prod" or env == "stg":
            raise ValueError("Cannot switch to stg or prod environments when running locally")
    os.system(f"az account set --subscription  DTS-DATAINGEST-{env.upper()}")
    populate_env_file(env)