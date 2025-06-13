## Purpose
- Contains a dockerfile to run redis with custom configuration.
- Contains openai_keypool service to allow borrowing open ai keys for consumers (external modules like data_etl_app).

## Usage
- Only import and use openai_keypool service. DO NOT IMPORT/USE ANYTHING ELSE.

## Setup
- Setup the redis server using Dockerfile in redis_infra
- Run seeds/open_ai_key_seed to populate redis with all keys.
- Refer to usage for interacting with the populated server.