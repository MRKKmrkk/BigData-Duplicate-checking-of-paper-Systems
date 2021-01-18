'''
验证码工具配置文件
'''

# 线程数
THRED_NUMBER = 20

# 训练次数
TRAIN_TIMES = 5000

# 验证码临时路径（不存在会自动创建）
CAPTCHA_TMP_PATH = "tmp\\"

#特征集的存储路径 （不存在会自动创建）
CAPTCHA_SAVE_PATH = "features\\"

# 验证码长度
CAPTCHA_LEN = 4

# 验证码长宽
CAPTCHA_SIZE = 1080, 480

# 生成验证码长宽
GEN_CAPTCH_SIZE = 122, 54

# 单个字符长宽
SINGLE_CHAR_SIZE = 35, 54

# 验证码阈值
CAPTCHA_THRESHOLD = 165

# 验证码降噪次数
CAPTCHA_REDUCATION_NOISE_TIME = 10

# 验证码后缀
CAPTCHA_SUFFIX = ".jpg"

# 组成验证码的所有字符集（要求数据类型为列表)
CAPTCHA_CHARS = list("1234567890" + "abcdefghijklmnopqrstuvwxyz" + "abcdefghijklmnopqrstuvwxyz".upper())

#特征及编码
FEATURES_ENCODING = "utf-8"
