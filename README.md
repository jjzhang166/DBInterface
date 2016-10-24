#功能说明
封装了常见数据库操作，通过构造函数的参数传入数据库的地址、用户名等信息。
# 对外提供接口
-  explicit DBInterfaceAIS( QMutex *mutex,StructDBInfo structDBInfo,QObject *parent = 0);
-  bool connectToDB();
-  bool hasFeatureOfTransaction();
-  bool startTransaction();
-  bool commitTransaction();

-  QSqlDatabase *db;
    //快速将多行数据批量插入表格
-  bool quickInsertInBatch(QSqlQuery query,QString tableName,
                      QList <QVariantList> &listColumnData,QString insertMethod="insert ignore");
    //将一行数据插入表格
-  bool insertOneRow(QSqlQuery query,QString tableName,QVariantList listData,QString insertMethod);

-  bool checkConnection();
-  QStringList getSourceDBTablePartitions(QString tableName);
-  QMutex *mutex;
-  QSqlQuery *query1;
# 说明
- 构造函数之所以传入了mutex，是为了防止两个线程同时构造两个数据库连接，否则会出现错误。
- 数据库插入操作应使用提供的两个插入语句，插入的数据量只要不大于服务器可用内存即可
- 其他的查询等操作，使用query1成员变量来进行
- 为了提高效率，已经将query1设置为forwardOnly
# 重要事项
- 为了防止出现MySQL Server has gone away这类错误，该类会定时查询一次数据库，执行show tables操作，避免连接断开。
- 为了让定时器能够正确触发，对象所在线程应该避免长时间连续耗时操作。可用的解决方案包括，每隔一段时间，执行一次qApp.processEvents()