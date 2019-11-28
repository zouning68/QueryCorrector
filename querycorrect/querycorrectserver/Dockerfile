# 基于镜像基础
FROM python:3.7.1

# 设置代码文件夹工作目录 /server
WORKDIR /server

# 复制当前代码文件到容器中 /server
ADD . /server

# 安装所需的包
#RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

EXPOSE 51668
VOLUME /server/log

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' > /etc/timezone

# Run run_server.sh when the container launches
#ENTRYPOINT ["./run_server.sh"]
CMD ["sh", "run_server.sh"]
