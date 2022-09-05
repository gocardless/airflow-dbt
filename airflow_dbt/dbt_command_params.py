# Python versions older than 3.8 have the TypedDict in a different namespace.
# In case we find ourselves in that situation, we use the `older` import
try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class DbtGlobalParamsConfig(TypedDict, total=False):
    """
    Holds the structure of a dictionary containing dbt config. Provides the
    types and names for each one, and also helps shortening the constructor
    since we can nest it and reuse it
    """
    record_timing_info: bool
    debug: bool
    log_format: str  # either 'text', 'json' or 'default'
    write_json: bool
    warn_error: bool
    partial_parse: bool
    use_experimental_parser: bool
    use_colors: bool
    verbose: bool
    no_use_colors: bool


class DbtCommandParamsConfig(TypedDict, total=False):
    """
    Holds the structure of a dictionary containing dbt config. Provides the
    types and names for each one, and also helps shortening the constructor
    since we can nest it and reuse it
    """
    profiles_dir: str
    project_dir: str
    target: str
    vars: dict
    models: str
    exclude: str

    # run specific
    full_refresh: bool
    profile: str

    # docs specific
    no_compile: bool

    # debug specific
    config_dir: str

    # ls specific
    resource_type: str  # models, snapshots, seeds, tests, and sources.
    select: str
    models: str
    exclude: str
    selector: str
    output: str
    output_keys: str

    # rpc specific
    host: str
    port: int

    # run specific
    fail_fast: bool

    # run-operation specific
    args: dict

    # test specific
    data: bool
    schema: bool
