wget https://www.python.org/ftp/python/3.11.0/Python-3.11.0.tgz
tar -xzvf Python-3.11.0.tgz
cd Python-3.11.0/
./configure --enable-optimizations; make -j `nproc`;
sudo make altinstall

python3.11 -V
python3.11 -m pip install --upgrade pip
python3.11 -m pip install virtualenv

python3.11 -m virtualenv -p /usr/local/bin/python3.11 venv
source venv/bin/activate
python --version
pip install --upgrade pip
pip install poetry
poetry install

playwright install