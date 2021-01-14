import os
from threading import Thread

from PIL import Image

import config
from captchaUtil import CaptchaCreator, CaptchaUtil


class Train(Thread):

    def __init__(self, sheduler, i):
        super().__init__()
        print("创建线程%d" % (i + 1))
        self.__sheduler = sheduler
        self.__capCreator = CaptchaCreator()
        self.__capUtil = CaptchaUtil()

    def run(self):
        for capName in self.__sheduler.getName():
            print("获取验证码", capName)
            charList = self.__capCreator.getRandomCharList()

            if config.CAPTCHA_TMP_PATH[-2:] == "\\\\":
                tmpPath = config.CAPTCHA_TMP_PATH + "\\\\"
            else:
                tmpPath = config.CAPTCHA_TMP_PATH
            path = tmpPath + capName + config.CAPTCHA_SUFFIX

            self.__capCreator.createCaptcha(charList, path)
            capImg = self.__capUtil.dealImg(path)
            try:
                charImgs = self.__capUtil.waterFeedSlice(capImg)
            except Exception as e:
                charImgs = None

            if charImgs:
                for i in range(len(charList)):
                    curImg = charImgs[i].resize((35, 54), Image.NEAREST)
                    self.__sheduler.save(charList[i], self.__capUtil.dimReducation(curImg))
            else:
                self.__sheduler.rollback1()
            os.remove(path)


if __name__ == '__main__':
    print("12345"[-2:])
    print("\\\\")

