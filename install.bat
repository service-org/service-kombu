:: 本地安装
python3 setup.py install
rmdir /s /q service_kombu.egg-info build dist

:: 打包分发
:: pip install -U setuptools wheel twine
:: python3 setup.py sdist bdist_wheel
:: twine upload dist/*
:: rmdir /s /q service_kombu.egg-info build dist
