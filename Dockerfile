FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc g++ \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY assets.json .
COPY market_data.py .
COPY signal_engine.py .
COPY macro_layer.py .
COPY fundamental_layer.py .
COPY scoring_engine.py .
COPY ai_validation.py .
COPY backtest_engine.py .
COPY smart_money.py .
COPY mailer.py .
COPY main.py .
COPY index.html .
COPY run.sh /run.sh
RUN chmod a+x /run.sh
CMD ["/run.sh"]
