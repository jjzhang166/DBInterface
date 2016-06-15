#include <QCoreApplication>
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

    return a.exec();
}
