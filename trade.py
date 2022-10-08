from datetime import datetime as dt
import csv
import os
import sys
from pydantic import ValidationError

my_path = os.path.realpath(os.path.dirname(__file__))
rel_path = "/../include"
sys.path.insert(0, my_path + rel_path)
from logger import Logger
from fileutils import Fileutils
from utilities import Utilities
from symbols import Symbols
from ohlcv import Heikenashi
from strategy import HaBreakout
from scripts import Scripts
from bypass import Bypass


class zha:
    def __init__(self):
        self.logger = Logger(20)
        self.scr_path = "scripts/"
        self.tick_file = "data/ticks.csv"
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
                # unique symbols
                if obj["base_script"] not in self.exchsym:
                    self.exchsym.append(obj["base_script"])
            except ValidationError as e:
                print(e.json())

        broker = self.f.get_lst_fm_yml("../confid/bypass.yaml")
        self.kite = Bypass(broker)
        if self.f.is_file_not_2day(self.tick_file):
            os.remove(self.tick_file)

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
            self.logger.exception("send_order {}, {}".format(err, type(err)))

    def close_trades(self, trade_list, script_obj, buy_or_sell):
        def fuzzy_match(fuzzy: str, match: str) -> bool:
            if fuzzy == match:
                return True
            first = fuzzy.split("(")
            self.logger.info(f"first is { first[0] }")
            last = fuzzy.split(")")
            self.logger.info(f"last is { last[1] }")
            flen = len(first[0])
            llen = len(last[1])
            if flen > 0 and llen > 0:
                self.logger.info("trading symbol contains first and last")
                if (first[0] == match[0:flen]) and (last[1] == match[(llen * -1) :]):
                    return True
                else:
                    self.logger.info(
                        f"match first { match[0:flen] }is not equal to {first[0] }"
                    )
                    self.logger.info(
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
                with open(self.tick_file, "a") as f:
                    writer = csv.writer(f)
                    writer.writerow(row)

            # get script objects and iterrate each
            for obj in self.objs:
                hab = HaBreakout(obj)
                ltp = hab.ltp
                if ltp:
                    obj["ltp"] = ltp
                cond = hab.cond()
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
