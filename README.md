# lagou_spider
此为拉钩网站的爬虫项目,项目环境如下：

> 数据库：Mysql8.0
>
> IDE：Pycharm
>
> Python3.7

# 使用流程：

1. Docker创建mysql8.0数据库容器

   ```shell
   docker run \
       -p 3333:3306 \
       -e MYSQL_ROOT_PASSWORD=123456 \
       -e TZ=Asia/Shanghai \
       -v ~/mydocker/mysql8/data:/var/lib/mysql:rw \
       -v ~/mydocker/mysql8/log:/var/log/mysql:rw \
       -v ~/mydocker/mysql8/config/my.cnf:/etc/mysql/my.cnf:rw \
       --name mysql8 \
       --restart=always \
       -d mysql:8.0
   ```

2. 创建数据库`lagou`
3. 运行`lagou_php_spider/create_laogou_tables.py`创建数据表
4. 运行`lagou_php_spider/handle_insert_data.py`爬取数据存放到数据库中
5. 运行`lagou_data_analysis/run.py`构建flask服务器,运行
6. 浏览器输入`http://0.0.0.0:12345/lagou/`打开页面