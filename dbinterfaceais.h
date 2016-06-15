#ifndef DBINTERFACEAIS_H
#define DBINTERFACEAIS_H

#include <QObject>
#include <QSqlDatabase>
#include <QMutex>

#define TABLE_NAME_FOR_CON_CHECK "DUAL" //用于测试数据库连接性

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

    bool checkConnection(QSqlDatabase * &dbParam);
    QStringList getSourceDBTablePartitions(QString tableName);
    QMutex *mutex;

signals:
    void sigShowInfo(QString str);
public slots:

private:
    bool reConnectToDB(QSqlDatabase * &dbParam);
    StructDBInfo dbInfo;
};

#endif // DBINTERFACEAIS_H
