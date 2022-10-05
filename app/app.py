import json
import logging
import random
import time
# add postgresql module
import psycopg
# for use zabbix_sender
import subprocess as sub

logging.basicConfig(
    level="INFO",
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
# add db cridentials
conn = psycopg.connect("dbname=test_db user=logger password=mWczmCRPzFtH")

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    while True:
        msg = dict()
        for level in range(50):
            (
                msg[f"bid_{str(level).zfill(2)}"],
                msg[f"ask_{str(level).zfill(2)}"],
            ) = (
                random.randrange(1, 100),
                random.randrange(100, 200),
            )
        msg["stats"] = {
            "sum_bid": sum(v for k, v in msg.items() if "bid" in k),
            "sum_ask": sum(v for k, v in msg.items() if "ask" in k),
        }
        logger.info(f"{json.dumps(msg)}")
        # write to db
        #cur = conn.cursor()
        #cur.execute("INSERT INTO logs (time, message) VALUES (%s, %s)", (time.strftime('%Y-%M-%D %H:%M:%S')+'.'+str(time.time()).split('.')[1], json.dumps(msg)))
        #conn.commit()
        # send alert to zabbix
        if msg.get('ask_01') + msg.get('bid_01') < 105:
            z_send = sub.Popen(['zabbix_sender', '-z', '127.0.0.1', '-s', 'hostname', '-k', 'log.alert', '-o', 'LOGS ALERT'],stdout=sub.PIPE,stderr=sub.PIPE)
            output, errors = z_send.communicate()
        time.sleep(0.001)
