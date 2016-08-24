#include <QDebug>
#include <QSqlError>
#include <qsqlquery.h>
#include <QSqlDriver>
#include <QMutex>
#include <QThread>
#include <QSqlRecord>
#include "dbinterfaceais.h"

DBInterfaceAIS::DBInterfaceAIS(QMutex *mutex, StructDBInfo structDBInfo, QObject *parent)
    : QObject(parent)
{
    db=NULL;
    this->mutex=mutex;
    dbInfo=structDBInfo;
}

bool DBInterfaceAIS::connectToDB()
{
    /*****connect to source database***/
    QMutexLocker locker(mutex);
    db = new QSqlDatabase(QSqlDatabase::addDatabase(dbInfo.type,dbInfo.connectionName));

    db->setHostName(dbInfo.hostIP);
    db->setDatabaseName(dbInfo.dbName); //Oracle的SID
    db->setUserName(dbInfo.userName);
    db->setPassword(dbInfo.pwd);

    if(!db->open())
    {
        emit sigShowInfo("Can't connect to database! "+db->lastError().text()
                         +" Connection name is "+db->connectionName());
        qDebug()<<"Can't connect to database! "+db->lastError().text()
                  +" Connection name is "+db->connectionName();
        return false;
    }
    else
    {
        emit sigShowInfo("Database is connected successfully! Connection name is "+db->connectionName());
        qDebug()<<"Database is connected successfully! Connection name is "+db->connectionName();
        return true;
    }    
}

//快速将大量数据插入表格
bool DBInterfaceAIS::quickInsertInBatch(QSqlQuery query, QString tableName, QList <QVariantList> listColumnData, QString insertMethod)
{
    query.setForwardOnly(true);
    qint16 columnCount=listColumnData.size();
    if(columnCount<=0)
        return false;

    QString prepareSQL=insertMethod+" into "+tableName+" values (";
    for(int i=0;i<columnCount;i++)
        prepareSQL.append("?,");

    prepareSQL.chop(1); //去掉最后一个逗号
    prepareSQL.append(")");

    if(!query.prepare(prepareSQL))
    {
        qDebug() <<query.lastError();
        sigShowInfo(query.lastError().text());
        return false;
    }

    while(!listColumnData.isEmpty())
    {
        QVariantList listRowData=listColumnData.takeFirst();
        query.addBindValue(listRowData);
    }

    if (!query.execBatch())
    {
        qDebug() <<query.lastError();
        sigShowInfo(query.lastError().text());
        return false;
    }
    else
        return true;
}

bool DBInterfaceAIS::insertOneRow(QSqlQuery query,QString tableName,QVariantList listData,QString insertMethod)
{
    query.setForwardOnly(true);
    qint16 columnCount=listData.size();
    if(columnCount<=0)
        return false;

    QString prepareSQL=insertMethod+" into "+tableName+" values (";
    for(int i=0;i<columnCount;i++)
        prepareSQL.append("?,");

    prepareSQL.chop(1); //去掉最后一个逗号
    prepareSQL.append(")");

    if(!query.prepare(prepareSQL))
    {
        qDebug() <<query.lastError();
        sigShowInfo(query.lastError().text());
        return false;
    }

    QListIterator <QVariant> iListData(listData);
    while(iListData.hasNext())
    {
        query.addBindValue(iListData.next());
    }

    if (!query.exec())
    {
        qDebug() <<query.lastError();
        sigShowInfo(query.lastError().text());
        return false;
    }
    else
        return true;
}

QStringList DBInterfaceAIS::getSourceDBTablePartitions(QString tableName)
{
    QStringList listPartitions;

    qint64 reTryConnectTimes=0;
    while(!checkConnection(db)) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }

    QSqlQuery query(*db);
    query.setForwardOnly(true);
    if(query.exec("explain partitions SELECT * FROM "+tableName))
    {
        if(query.next())
        {
            qint16 index=query.record().indexOf("partitions");
            if(index>=0)
            {
                return query.value(index).toString().split(',');
            }
        }
        return listPartitions;
    }
    else
    {
        emit sigShowInfo("Database error when explain partitions. "+query.lastError().text());
        return listPartitions;
    }
}


DBInterfaceAIS::~DBInterfaceAIS()
{
    if(db->isOpen())
    {
        db->close();
        delete db;
        db=NULL;
        QSqlDatabase::removeDatabase(dbInfo.connectionName);
    }
}

bool DBInterfaceAIS::hasFeatureOfTransaction()
{
    return db->driver()->hasFeature(QSqlDriver::Transactions);
}

bool DBInterfaceAIS::startTransaction()
{
    return db->transaction();
}

bool DBInterfaceAIS::commitTransaction()
{
    return db->commit();
}

bool DBInterfaceAIS::checkConnection(QSqlDatabase * &dbParam)
{
    if(dbParam==NULL||db==NULL)
    {
        return false;
    }

    if(dbParam->isOpen())
        return true;

    StructDBInfo dbInfo;
    dbInfo.connectionName=dbParam->connectionName();
    dbInfo.dbName=dbParam->databaseName();
    dbInfo.hostIP=dbParam->hostName();
    dbInfo.pwd=dbParam->password();
    dbInfo.userName=dbParam->userName();
    dbInfo.type=dbParam->driverName();
    if(dbParam->isOpen())
    {
        QSqlQuery query(*dbParam);
        if(query.exec("select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1"))
            return true;
        else
        {
            return reConnectToDB(dbParam);
        }
    }
    else if(!dbParam->open())
    {
        emit sigShowInfo("Can't connect to  database! Connection name is:"+dbParam->connectionName()+
                         dbParam->lastError().text());
        return reConnectToDB(dbParam);

    }
    else
    {
        emit sigShowInfo("Database is reConnected successfully! Connection name is:"+dbParam->connectionName());
        return true;
    }
}

bool DBInterfaceAIS::reConnectToDB(QSqlDatabase * &dbParam)
{
    QSqlDatabase::removeDatabase(dbParam->connectionName());
    dbParam->close();
    delete dbParam;
    dbParam=NULL;

    qDebug()<<"Adding database:"<<dbInfo.connectionName;
    dbParam = new QSqlDatabase(QSqlDatabase::addDatabase(dbInfo.type,dbInfo.connectionName));
    dbParam->setHostName(dbInfo.hostIP);
    dbParam->setDatabaseName(dbInfo.dbName); //Oracle的SID
    dbParam->setUserName(dbInfo.userName);
    dbParam->setPassword(dbInfo.pwd);
    if(!dbParam->open())
    {
        emit sigShowInfo("Can't connect to  database! "+dbParam->lastError().text()+
                         " Connection name is "+dbParam->connectionName());
        qDebug()<<"Can't coonect to db:"+dbParam->connectionName();
        return false;
    }
    else
    {
        emit sigShowInfo("database is connected successfully!"+dbParam->lastError().text()+
                         " Connection name is "+dbParam->connectionName());
    }

    QSqlQuery query(*dbParam);
    if(query.exec("select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1"))
        return true;
    else
        return false;
}
