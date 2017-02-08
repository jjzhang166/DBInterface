QT += core sql
QT -= gui

INCLUDEPATH += $$PWD/Module_Qt_DBInterface
CONFIG += c++11

TARGET = DBInterface
CONFIG += console
CONFIG -= app_bundle

TEMPLATE = app

SOURCES += main.cpp \
    Module_Qt_DBInterface/dbinterfaceais.cpp

HEADERS += \
    Module_Qt_DBInterface/dbinterfaceais.h
