#include <QDebug>
#include <QSqlError>
#include <qsqlquery.h>
#include <QSqlDriver>
#include <QMutex>
#include <QThread>
#include <QSqlRecord>
#include "dbinterfaceais.h"
#include <QtMath>
#include <QTimer>

DBInterfaceAIS::DBInterfaceAIS(QMutex *mutex, StructDBInfo structDBInfo, QObject *parent)
    : QObject(parent)
{
    db=NULL;
    query1=NULL;
    timerCheckConnection=new QTimer(this);
    connect(timerCheckConnection,SIGNAL(timeout()),
            this,SLOT(slotTimerEventCheckConnection()));
    timerCheckConnection->start(3600000);//一小时检查一次
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
        query1=new QSqlQuery(*db);
        query1->setForwardOnly(true);
        return true;
    }
}

void DBInterfaceAIS::slotTimerEventCheckConnection()
{
    qint64 reTryConnectTimes=0;
    qDebug()<<"Checking connection with MySQL...";
    while(!checkConnection(db)) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        qDebug()<<"Connection to source database  failed!"+db->lastError().text()
                  +" COnnection name is "+db->connectionName()
                  +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds";
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }
    qDebug()<<"Connection with MySQL is ok!";
}

//快速将大量数据插入表格。参数通过引用传递，减少内存占用
bool DBInterfaceAIS::quickInsertInBatch(QSqlQuery query, QString tableName,
                        QList<QVariantList> &listColumnData, QString insertMethod)
{
    qint64 reTryConnectTimes=0;
    while(!checkConnection(db)) //检测连接是否还在
    {
        reTryConnectTimes++;
        emit sigShowInfo("Connection to source database  failed!"+db->lastError().text()
                         +" COnnection name is "+db->connectionName()
                         +" This therad will sleep "+QByteArray::number(pow(4,reTryConnectTimes))+" seconds");
        this->thread()->sleep(pow(4,reTryConnectTimes)); //等待
    }
    qint16 columnCount=listColumnData.size();
    if(columnCount<=0)
        return false;

    QString prepareSQL=insertMethod+" into "+tableName+" values (";
    for(int i=0;i<columnCount;i++)
        prepareSQL.append("?,");

    prepareSQL.chop(1); //去掉最后一个逗号
    prepareSQL.append(")");

    qint64 rowCount=listColumnData.first().size();//行数
    qint64 rowIndex=0;
    if(rowCount<=0)
        return false;

    if(!query.prepare(prepareSQL))
    {
        qDebug() <<prepareSQL<<". Error info:"<< query.lastError().text()<<endl;
        sigShowInfo(query.lastError().text());
        return false;
    }

    while(rowIndex<rowCount)//一次插入太多会导致超出数据库的max_allowed_packet
    {
        for(int indexColumn=0;indexColumn<columnCount;indexColumn++)
        {
            QVariantList listDataOfOneColumn=listColumnData.at(indexColumn)
                    .mid(rowIndex,ROWS_OF_SINGLE_BATCH);
            query.addBindValue(listDataOfOneColumn);
        }
        rowIndex+=ROWS_OF_SINGLE_BATCH;
        if (!query.execBatch())
        {
            qDebug() <<"Fail to execBatch:"<<prepareSQL<<query.lastError().text();
            sigShowInfo(query.lastError().text());
            return false;
        }
        if(rowIndex/ROWS_OF_SINGLE_BATCH%10==0)
            qDebug()<<"Value of current rowIndex:"<<rowIndex;
    }

    return true;
}

bool DBInterfaceAIS::insertOneRow(QSqlQuery query,QString tableName,QVariantList listData,QString insertMethod)
{
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
        qDebug() <<prepareSQL<<". Error info:"<< query.lastError().text()<<endl;
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
        qDebug() <<prepareSQL<<". Error info:"<<query.lastError();
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
        QString strSQL;
        //strSQL="select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1";
        strSQL="show tables";
        if(query.exec(strSQL))
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
        qDebug()<<"Can't connect to  database! Connection name is:"+dbParam->connectionName()+
                  dbParam->lastError().text();
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
        qDebug()<<"database is connected successfully!"+dbParam->lastError().text()+
                  " Connection name is "+dbParam->connectionName();
    }

    QSqlQuery query(*dbParam);
    QString strSQL;
    //strSQL="select 1 from " TABLE_NAME_FOR_CON_CHECK " limit 1";
    strSQL="show tables";
    if(query.exec(strSQL))
        return true;
    else
        return false;
}
