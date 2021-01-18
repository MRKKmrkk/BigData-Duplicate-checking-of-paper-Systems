import os
import uuid
from threading import Lock
import config
from train import Train
from random import choice


class Sheduler:

    def __init__(self):
        self.lock = Lock()
        self.trainTimes = config.TRAIN_TIMES

        if config.CAPTCHA_SAVE_PATH[-2:] == "\\\\":
            savePath = config.CAPTCHA_SAVE_PATH + "\\\\"
        else:
            savePath = config.CAPTCHA_SAVE_PATH

        self.files = {}
        for char in config.CAPTCHA_CHARS:
            path = savePath + char
            mode = "w"
            if os.path.exists:
                mode = "a"
            self.files[char] = open(path, mode, encoding=config.FEATURES_ENCODING)

        self.collectNum = 0

    '''
    获取随机名称
    '''

    def __getRandomName(self):
        return str(uuid.uuid4().hex)

    def getName(self):
        for i in range(self.trainTimes):
            # self.lock.acquire()
            yield self.__getRandomName()
            # self.lock.release()

    def rollback1(self):
        self.lock.acquire()
        print("回滚验证码")
        self.trainTimes += 1
        self.lock.release()

    def save(self, char, feature):
        self.lock.acquire()
        self.files[char].write(" ".join(feature) + "\n")
        self.collectNum += 1
        print("记录第%d个特征,%s" % (self.collectNum, char))
        self.lock.release()

    def close(self):
        for key in self.files:
            self.files[key].close()

def init():
    if os.path.exists(config.CAPTCHA_TMP_PATH):
        if not os.path.isdir(config.CAPTCHA_TMP_PATH):
            raise Exception("CAPTCHA_TMP_PATH参数必须指定一个目录")
    else:
        os.mkdir(config.CAPTCHA_TMP_PATH)

    if os.path.exists(config.CAPTCHA_SAVE_PATH):
        if not os.path.isdir(config.CAPTCHA_SAVE_PATH):
            raise Exception("CAPTCHA_SAVE_PATH参数必须指定一个目录")
    else:
        os.mkdir(config.CAPTCHA_SAVE_PATH)

    if config.CAPTCHA_TMP_PATH[-2:] == "\\\\":
        tmpPath = config.CAPTCHA_TMP_PATH + "\\\\"
    else:
        tmpPath = config.CAPTCHA_TMP_PATH
    for f in os.listdir(tmpPath):
        os.remove(tmpPath + f)


if __name__ == '__main__':
    print("初始化环境...")
    init()

    print("开始训练")
    sheduler = Sheduler()

    for i in range(config.THRED_NUMBER):
        Train(sheduler, i).start()

    # sheduler.close()

