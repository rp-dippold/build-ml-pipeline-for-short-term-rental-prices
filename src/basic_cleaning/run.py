#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import os

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Downloading input artifact {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Open input artifact
    df = pd.read_csv(artifact_local_path)

    # Drop outliers regarding the price variable
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Select rows that comply to value boundaries regarding 'longitude' and 'latitude'
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # Save dataframe in a temporary file
    filename = "clean_sample.csv"
    df.to_csv(filename, index=False)

    # Upload output artifact to W&B
    artifact = wandb.Artifact(
        args.output_name,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)
    logger.info("Logging artifact")
    run.log_artifact(artifact)

    # Remove temporary file
    os.remove(filename)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_name", 
        type=str,
        help="Name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Lower price boundary to be considered",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Upper price boundary to be considered",
        required=True
    )


    args = parser.parse_args()

    go(args)
