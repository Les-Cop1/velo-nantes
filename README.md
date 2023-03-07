# velo-nantes

Python script to import datas about number of bikes in Nantes depending on temperature.

## Installation

### Virtualenv setup
    
```bash
python -m venv venv
source venv/bin/activate
```

### Dependencies installation

```bash
python -m pip install -r requirements.txt
```

### Environment variables

You need to set the following environment variables

```bash
cp .env.example .env
```

Then edit the `.env` file with your own values

### Add execution rights

```bash
chmod +x ./velo-nantes
```


## Usage

To use this script run the following command

```bash
./velo-nantes
```