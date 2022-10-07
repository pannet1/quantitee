import logging

temp = "[%(asctime)s] %(levelname)s {%(pathname)s:%(lineno)d} - %(message)s]"
logging.basicConfig(
    filename="info.log",
    filemode="w",
    format=temp,
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
)
from time import sleep
from datetime import datetime as dt
import csv
from kiteext import KiteExt
import pyotp
import os
import sys

my_path = os.path.realpath(os.path.dirname(__file__))
rel_path = "/../include"
sys.path.insert(0, my_path + rel_path)
from fileutils import Fileutils
from utilities import Utilities
from symbols import Symbols
from ohlcv import Heikenashi
from scripts import Scripts
from pydantic import ValidationError
from strategy import HaBreakout


class zha:
    def __init__(self):
        self.scr_path = "scripts/"
        self.secs = -1
        self.s, self.u, self.f = Symbols(), Utilities(), Fileutils()
        self.exchsym, self.objs = [], []
        ymlfiles = self.f.get_files_with_extn("yaml", self.scr_path)
        for y in ymlfiles:
            obj = self.f.get_lst_fm_yml(self.scr_path + y)
            try:
                # validate scripts
                Scripts(**obj)
                obj["trade_cond"] = ""
                self.objs.append(obj)
                if obj["base_script"] not in self.exchsym:
                    self.exchsym.append(obj["base_script"])
            except ValidationError as e:
                print(e.json())

        broker = self.f.get_lst_fm_yml("../confid/bypass.yaml")
        otp = pyotp.TOTP(broker["totp"])
        pin = otp.now()
        if len(pin) <= 5:
            pin = f"{int(pin):06d}"
        self.kite = KiteExt()
        self.kite.login_with_credentials(
            userid=broker["username"], password=broker["password"], pin=pin
        )

    def place_order(self, obj):
        try:
            order = self.kite.place_order(
                exchange=obj["exchange"],
                tradingsymbol=obj["trading_symbol"],
                transaction_type=obj["transaction_type"],
                quantity=obj["quantity"],
                product=obj["product"],
                order_type="MARKET",
                price=0,
                trigger_price=0,
                variety=self.kite.VARIETY_REGULAR,
                tag="algo",
            )
        except BaseException as err:
            logging.exception("send_order {}, {}".format(err, type(err)))

    def get_positions(self):
        pos = self.kite.positions()
        self.MIS = pos["net"]
        self.NRML = pos["day"]

    def close_trades(self, trade_list, script_obj, buy_or_sell):
        def fuzzy_match(fuzzy: str, match: str) -> bool:
            if fuzzy == match:
                return True
            first = fuzzy.split("(")
            logging.info(f"first is { first[0] }")
            last = fuzzy.split(")")
            logging.info(f"last is { last[1] }")
            flen = len(first[0])
            llen = len(last[1])
            if flen > 0 and llen > 0:
                logging.info("trading symbol contains first and last")
                if (first[0] == match[0:flen]) and (last[1] == match[(llen * -1) :]):
                    return True
                else:
                    logging.info(
                        f"match first { match[0:flen] }is not equal to {first[0] }"
                    )
                    logging.info(
                        f"last { last[1] } is not equal to { match[(llen*-1):] }"
                    )
            return False

        fuzzy_sym = (
            script_obj["buy_script"]
            if buy_or_sell == "BUY"
            else script_obj["sell_script"]
        )
        tx = script_obj["buy_tx"] if buy_or_sell == "BUY" else script_obj["sell_tx"]

        for t in trade_list:
            if script_obj["product"] == t["product"]:
                qty = (
                    t["overnight_quantity"] if t["product"] == "NRML" else t["quantity"]
                )
                if qty > 0 or qty < 0:
                    pos = "BUY" if qty > 0 else "SELL"
                    if pos == tx:
                        ## find script which is of the form @@@@(@@@)@@@@@
                        if fuzzy_match(fuzzy_sym, t["tradingsymbol"]):
                            if t["trade"] == tx:
                                t["transaction_type"] = (
                                    "SELL" if tx == "BUY" else "SELL"
                                )
                                self.place_order(t)
                                print("TODO: Place order fuzzy match is True")

    def run(self):
        while self.secs != dt.now().second:
            resp = self.kite.ltp(self.exchsym)
            for k, v in resp.items():
                compo = k.split(":")
                row = dt.now(), compo[1], v["last_price"]
                with open("data/ticks.csv", "a") as f:
                    writer = csv.writer(f)
                    writer.writerow(row)

            # get script objects and iterrate each
            for obj in self.objs:
                hab = HaBreakout(obj)
                ltp = hab.ltp
                if ltp:
                    obj['ltp'] = ltp
                cond = hab.cond()
                '''
                ha = Heikenashi(obj)
                ltp = ha.get_ltp()
                if ltp:
                    obj["ltp"] = ltp
                if len > 1:
                    curr_ha = ha.candle(1)
                    cond = self.trade_cond(ha.candle(2), curr_ha)
                '''
                # TODO
                # pos = self.f.json_fm_file(obj['product'])
                # self.close_trades(pos, obj, "SELL")
                # buy condition
                if cond == 1:
                    # buy entry
                    if obj["trade_cond"] == "":
                        obj["trading_symbol"] = self.s.set_trd_sym("B", obj)
                        obj["trasaction_type"] = obj["buy_tx"]
                        obj["trade_cond"] = "BUY"
                        print(obj)
                        self.place_order(obj)
                    # cover short trade
                    elif obj["trade_cond"] == "SELL":
                        obj["transaction_type"] = (
                            "SELL" if obj["buy_tx"] == "BUY" else "BUY"
                        )
                        print(obj)
                        self.place_order(obj)
                elif cond == -1:
                    # sell entry
                    if obj["trade_cond"] == "":
                        obj["trading_symbol"] = self.s.set_trd_sym("S", obj)
                        obj["transaction_type"] = obj["sell_tx"]
                        obj["trade_cond"] = "SELL"
                        print(obj)
                        self.place_order(obj)
                    elif obj["trade_cond"] == "BUY":
                        obj["transaction_type"] = (
                            "BUY" if obj["sell_tx"] == "SELL" else "SELL"
                        )
                        print(obj)
                        self.place_order(obj)

            self.u.slp_til_nxt_sec()


zha().run()
