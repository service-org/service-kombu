# 本地安装
sudo python3 setup.py install
sudo rm -rf service_kombu.egg-info build dist

# 打包分发
# sudo pip install -U setuptools wheel twine
# sudo python3 setup.py sdist bdist_wheel
# sudo twine upload dist/*
# sudo rm -rf service_kombu.egg-info build dist
