#include <QCoreApplication>
#include <QSqlQuery>
#include <QVariant>
#include <QDebug>
#include "dbinterfaceais.h"

/*****main函数的参数依次为：数据库用户名、密码、数据库驱动类型**************/
int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);
    QMutex *mutex=new QMutex();
    StructDBInfo dbInfo;
    dbInfo.connectionName="Test";
    dbInfo.dbName=argv[3];
    dbInfo.hostIP="127.0.0.1";
    dbInfo.port=3306;
    dbInfo.pwd=argv[2];
    dbInfo.type="QMYSQL";
    dbInfo.userName=argv[1];
    DBInterfaceAIS *dbi=new DBInterfaceAIS(mutex,dbInfo);
    dbi->connectToDB();
    dbi->checkConnection();
    dbi->reConnectToDB();
    QSqlQuery query(*(dbi->db));
    query.setForwardOnly(true);
    QVariantList listCol1,listCol2;
    listCol1<<5<<6<<7<<8<<9;
    listCol2<<6<<5<<3<<3<<2;
    QList <QVariantList> listData;
    listData.append(listCol1);
    listData.append(listCol2);
    if(!dbi->quickInsertInBatch(query,"test_insert",listData))
        qDebug()<<"Fail to insert";
    else
        qDebug()<<"Insert succefull";

    return a.exec();
}
