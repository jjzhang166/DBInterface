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
    dbInfo.pwd=argv[2];
    dbInfo.type="QPSQL";
    dbInfo.userName=argv[1];
    DBInterfaceAIS *dbi=new DBInterfaceAIS(mutex,dbInfo);
    dbi->connectToDB();
    QSqlQuery query(*(dbi->db));
    query.setForwardOnly(true);
    QVariantList listCol1,listCol2;
    listCol1<<5<<6;
    listCol2<<6<<5;
    QList <QVariantList> listData;
    listData.append(listCol1);
    listData.append(listCol2);
    if(!dbi->quickInsertInBatch(query,"public.test_primary_key",listData))
        qDebug()<<"Fail to insert";
    else
        qDebug()<<"Insert succefull";

    return a.exec();
}
