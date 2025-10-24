build & install with uv:
```
uv build --no-cache
uv tool install --python PYTHON_PATH --compile-bytecode file:dist/cdp_metric_collector-x.x.x-py3-none-any.whl
# with kerberos support
uv tool install --python PYTHON_PATH --compile-bytecode 'cdp-metric-collector[kerberos] @ file:dist/cdp_metric_collector-x.x.x-py3-none-any.whl'
```
