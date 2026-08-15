import argparse
import json
import os

import yaml


def _set_env_var(e: str) -> None:
    key, val = e.split("=", 1)
    os.environ[f"GOESDATABUILDER__{key.upper()}"] = val


def cli() -> None:
    """Command line interface."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--type", choices=["GOES"], type=str.upper, default="GOES", help=argparse.SUPPRESS
    )  # TODO: remove default and argparse.SUPPRESS when adding more types
    parser.add_argument("-c", "--config", action="append", default=[])
    parser.add_argument("--config-dict", type=yaml.safe_load, default="{}")
    parser.add_argument("-e", "--env-config", action="append", type=_set_env_var)

    subparsers = parser.add_subparsers(dest="action")

    subparsers.add_parser("weights")
    subparsers.add_parser("nc2zarr")

    config_subparser = subparsers.add_parser("config")
    config_subparser.add_argument("-f", "--format", choices=["json", "yaml"], default="json")

    args = parser.parse_args()

    from goesdatabuilder import GeostationaryRegridder, config, get_config

    if args.type == "GOES":
        from goesdatabuilder import GOESMultiCloudObservation as Observation
        from goesdatabuilder import GOESPipelineOrchestrator as Pipeline

    with config(*args.config, config_dict=args.config_dict):
        # sample_size=0 so that validate runs to check if the files exist but not to check all the files since we
        # just need one to build the weights.
        if args.action == "weights":
            if not (weights_dir := get_config()["regridding"]["weights_dir"]):
                raise Exception(
                    "Cannot save weights: no directory specified. "
                    "Set weights_dir in the configuration file or use the -e flag. e.g. "
                    "-e regridding__weights_dir=/some/path"
                )
            observation = Observation(sample_size=0).first

            Pipeline(lazy_init=True).initialize_regridder(weights_dir=None)
            regridder = GeostationaryRegridder.from_observation(observation)
            regridder.save_weights(weights_dir)
        elif args.action == "nc2zarr":
            Pipeline().process_all()
        else:
            config_dict = get_config().to_dict()
            if args.format == "json":
                print(json.dumps(config_dict))
            else:
                print(yaml.safe_dump(config_dict))
