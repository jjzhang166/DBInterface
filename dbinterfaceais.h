#ifndef DBINTERFACEAIS_H
#define DBINTERFACEAIS_H

#include <QObject>
#include <QSqlDatabase>
#include <QMutex>

//#define TABLE_NAME_FOR_CON_CHECK "DUAL" //用于测试数据库连接性

#define BATCHES_OF_SINGLE_TRANSACTION 1000 //一个事务中多少个batch
#define ROWS_OF_SINGLE_BATCH 100000 //一个Batch导入多少行数据

class QSqlQuery;
class QTimer;
struct StructDBInfo
{
    QString type;
    QString hostIP;
    QString userName;
    QString pwd;
    QString dbName;
    QString connectionName;
};

class DBInterfaceAIS : public QObject
{
    Q_OBJECT
public:
    explicit DBInterfaceAIS( QMutex *mutex,StructDBInfo structDBInfo,QObject *parent = 0);
    bool connectToDB();
    ~DBInterfaceAIS();

    bool hasFeatureOfTransaction();
    bool startTransaction();
    bool commitTransaction();

    QSqlDatabase *db;
    //快速将多行数据批量插入表格
    bool quickInsertInBatch(QSqlQuery query,QString tableName,
                      QList <QVariantList> &listColumnData,QString insertMethod="insert ignore");
    //将一行数据插入表格
    bool insertOneRow(QSqlQuery query,QString tableName,QVariantList listData,QString insertMethod);

    bool checkConnection(QSqlDatabase * &dbParam);
    QStringList getSourceDBTablePartitions(QString tableName);
    QMutex *mutex;
    QSqlQuery *query1;

signals:
    void sigShowInfo(QString str);
public slots:
    void slotTimerEventCheckConnection();

private:
    bool reConnectToDB(QSqlDatabase * &dbParam);
    StructDBInfo dbInfo;
    QTimer *timerCheckConnection;
};

#endif // DBINTERFACEAIS_H
