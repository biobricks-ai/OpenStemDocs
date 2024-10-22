stages:
  loadurls:
    cmd: python stages/01_get_openaccess.py
    deps:
    - stages/01_get_openaccess.py
    outs:
    - brick:
        persist: true
  u00:
    cmd: python stages/02_download0.py
    deps:
    - stages/brick/url_00.parquet
    outs:
    - u00:
        persist: True
  u01:
    cmd: python stages/02_download1.py
    deps:
    - stages/brick/url_01.parquet
    outs:
    - u01:
        persist: True
  u02:
    cmd: python stages/02_download2.py
    deps:
    - stages/brick/url_02.parquet
    outs:
    - u02:
        persist: True
  u03:
    cmd: python stages/02_download3.py
    deps:
    - stages/brick/url_03.parquet
    outs:
    - u03:
        persist: True
