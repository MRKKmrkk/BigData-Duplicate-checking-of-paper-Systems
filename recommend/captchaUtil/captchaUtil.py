from PIL import Image
from captcha.image import ImageCaptcha

import config
from random import choice


class CaptchaCreator:

    def __init__(self):
        self.chars = config.CAPTCHA_CHARS

    '''
    获取验证码字符列表
    '''

    def getRandomCharList(self):
        result = []

        for i in range(config.CAPTCHA_LEN):
            result.append(choice(self.chars))

        return result

    '''
    创建验证码
    '''

    def createCaptcha(self, l, path):
        img = ImageCaptcha(width=config.GEN_CAPTCH_SIZE[0], height=config.GEN_CAPTCH_SIZE[1])
        img.generate(l)
        img.write(l, path)


class CaptchaUtil:
    '''
    加载图片对象
    '''

    def getImg(self, path):
        img = Image.open(path)
        return img

    '''
    将img对象保存为图片
    '''

    def saveImg(self, img, path):
        img.save(path)

    '''
    将彩色图片转换为灰度图片
    '''

    def __toGrayImg(self, img):
        imgGary = img.convert('L')
        return imgGary

    '''
    将灰度图片二值化
    '''

    def __toBinImg(self, imgGray, threshold=config.CAPTCHA_THRESHOLD):  # 165
        pixdata = imgGray.load()
        w, h = imgGray.size
        for y in range(h):
            for x in range(w):
                if pixdata[x, y] < threshold:
                    pixdata[x, y] = 0
                else:
                    pixdata[x, y] = 255
        return imgGray

    '''
    对图片进行降噪,可进行多轮降噪
    '''

    def __noiseReduction(self, binImg):
        pixdata = binImg.load()
        w, h = binImg.size
        for y in range(1, h - 1):
            for x in range(1, w - 1):
                count = 0
                # 上
                if pixdata[x, y - 1] > 245:
                    count = count + 1
                # 下
                if pixdata[x, y + 1] > 245:
                    count = count + 1
                # 左
                if pixdata[x - 1, y] > 245:
                    count = count + 1
                # 右
                if pixdata[x + 1, y] > 245:
                    count = count + 1
                # 左上
                if pixdata[x - 1, y - 1] > 245:
                    count = count + 1
                # 左下
                if pixdata[x - 1, y + 1] > 245:
                    count = count + 1
                # 右上
                if pixdata[x + 1, y - 1] > 245:
                    count = count + 1
                # 右下
                if pixdata[x + 1, y + 1] > 245:
                    count = count + 1
                if count > 4:
                    pixdata[x, y] = 255
        return binImg

    def dealImg(self, path):
        img = self.getImg(path)
        grayImg = self.__toGrayImg(img)
        binImg = self.__toBinImg(grayImg)
        for i in range(config.CAPTCHA_REDUCATION_NOISE_TIME):
            binImg = self.__noiseReduction(binImg)
        return binImg

    '''
    通过洪水填充算法或者叫DFS算法
    寻找出所有字符
    '''

    def __recursiveFindChar(self, x, y, pixs, curCharPix, w, h):
        if (0 <= x < w) and (0 <= y < h):
            if pixs[x][y] == 0:
                curCharPix.append((x, y))
                pixs[x][y] = 1

                self.__recursiveFindChar(x, y + 1, pixs, curCharPix, w, h)
                self.__recursiveFindChar(x, y - 1, pixs, curCharPix, w, h)
                self.__recursiveFindChar(x + 1, y, pixs, curCharPix, w, h)
                self.__recursiveFindChar(x - 1, y, pixs, curCharPix, w, h)

    '''
    对标记的字符集位置进行切割
    '''

    def __slice(self, cleanCharPixs, img, h, w):
        result = []

        for charPix in cleanCharPixs:
            xs = [x[0] for x in charPix]
            ys = [x[1] for x in charPix]

            xMax = max(xs)
            yMax = max(ys)
            xMin = min(xs)
            yMin = min(ys)
            print((xMin, yMax, xMax, yMin))

            result.append(img.crop((yMin, h - xMax, yMax, xMax)))
        return result

    '''
    过滤多余的字符
    '''

    def __filterChars(self, charPixs):
        result = []

        charsLens = sorted([len(x) for x in charPixs], reverse=True)
        if len(charsLens) > config.CAPTCHA_LEN:
            charsLens = charsLens[:config.CAPTCHA_LEN]

        for pix in charPixs:
            if len(pix) in charsLens:
                result.append(pix)

        return result

    '''
    切割验证码
    '''
    def waterFeedSlice(self, img):
        img.resize = config.CAPTCHA_SIZE

        w, h = img.size
        tmpPix = img.load()
        pixs = []

        for y in range(0, h):
            line = []
            for x in range(0, w):
                line.append(tmpPix[x, y])
            pixs.append(line)

        allCharsPixs = []
        for y in range(1, w - 1):  # h
            for x in range(1, h - 1):  # w
                curCharPix = []
                self.__recursiveFindChar(x, y, pixs, curCharPix, h, w)  # wh
                if len(curCharPix) > 0:
                    allCharsPixs.append(curCharPix)

        cleanCharPix = self.__filterChars(allCharsPixs)
        if len(cleanCharPix) == config.CAPTCHA_LEN:
            return self.__slice(cleanCharPix, img, h, w)

    '''
    降维生成特征码
    '''
    def dimReducation(self, img):
        pixdata = img.load()
        ans = []

        w, h = img.size
        for y in range(1, h - 1):
            for x in range(1, w - 1):
                pix = pixdata[x, y]
                if pix > 0:
                    pix = 1
                ans.append(str(pix))
        return ans


if __name__ == '__main__':
    while True:
        i = input(">>>")
        c = CaptchaCreator()
        chars = c.getRandomCharList()
        c.createCaptcha(chars, "t.jpg")

        u = CaptchaUtil()
        img = u.dealImg("t.jpg")
        charImgs = u.waterFeedSlice(img)

        if charImgs:
            for i in charImgs:
                i.show()
        else:
            print("empty")

