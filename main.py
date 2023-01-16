
import Meter as M
from fastapi import FastAPI
import uvicorn
#import json
#import Api as A


if __name__ == "__main__":
        app = FastAPI()
        """
        meters = []
        data = []
        all = M.getallserialnumbers()
        print("processing...")
        for i in range(10):
                meters.append(M.Meter(all[i]))
                payload = meters[i].getdatarawbytes()
                payloadtolist = meters[i].payloadtolist(payload)
                lastvolume = meters[i].getlastvolume(payloadtolist)
                time = meters[i].getmeterunixtime(payloadtolist)
                currentflag = meters[i].getcurrentflag(payloadtolist)
                thismonthflag = meters[i].getthismonthflag(payloadtolist)
                lastmonthflag = meters[i].getlastmonthfag(payloadtolist)
                data.append(f"{meters[i].getsn()},{time}, {lastvolume}, {lastmonthflag}, {thismonthflag}, {currentflag}")
                #print(lastvolume)
                """


        @app.get("/all")
        async def read_root():
                all = M.getallserialnumbers()
                return all

        @app.get("/{serialnumber}") ## 8APA0180204556, 8APA0180204551
        async def read_root(serialnumber):
                try:
                        m = M.Meter(serialnumber)
                        payload = m.getdatarawbytes()
                        payloadtolist = m.payloadtolist(payload)
                        lastvolume = m.getlastvolume(payloadtolist)
                        time = m.getmeterunixtime(payloadtolist)
                        currentflag = m.getcurrentflag(payloadtolist)
                        thismonthflag = m.getthismonthflag(payloadtolist)
                        lastmonthflag = m.getlastmonthfag(payloadtolist)
                        reverse = m.getreversevol(payloadtolist)
                        deveui = m.getdeveui()
                        volumes = m.get13volume(payloadtolist, lastvolume)
                except: return "No Meter with this SN"

                return {"Serial_Number" : f"{serialnumber}",
                        "Dev_EUI" : f"{deveui}",
                        "Actual_Volume" : f"{lastvolume}",
                        "log_time" : f"{time}",
                        "Currentflag" : f"{currentflag}",
                        "24h_flag" : f"{thismonthflag}",
                        "Current_month_flag" : f"{lastmonthflag}",
                        "Reverse_volume" : f"{reverse}",
                        "volumes_last_13_hours" : f"{volumes}",
                        "Raw_payload" : f"{payload}"}


        uvicorn.run(app, host="0.0.0.0", port=8000)
        """
                     
        for obj in meters:
                payload = obj.getdatarawbytes()
                payloadtolist = M.Meter.payloadtolist(payload)
                lastvolume = M.Meter.getlastvolume(payloadtolist)
                print(lastvolume)
                """
        """
        print(M.Meter.getmeterunixtime(payloadtolist))
        print(f"Last Volume log {lastvolume} m3")
        print(f"Last month flags: {lastmontflags}")
        print(f"This month flags: {thismonthflags}")
        print(f"Current flags: {currentflags}")
        #print(M.getallserialnumbers())
        """

