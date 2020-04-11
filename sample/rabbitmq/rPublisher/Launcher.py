import time

from PayloadGen import getpayload, init, getrandpayload
from Publisher import pub



print("prenit")
init()
print("posnit")
while True:
    print("we in here")
    payload = getrandpayload()
    pub(payload)
    time.sleep(2)
