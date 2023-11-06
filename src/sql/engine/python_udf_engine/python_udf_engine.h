#define PY_SSIZE_T_CLEAN
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#ifndef _PYTHON_UDF_ENGINE_H_
#define _PYTHON_UDF_ENGINE_H_
#include <Python.h>
#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>
#include <map>
#include <string>
#include <exception>
#include <sys/time.h>
#include <frameobject.h>
#include <fstream>
#include <sys/syscall.h>
#include "share/datum/ob_datum_util.h"

#include "sql/engine/python_udf_engine/python_udf_schema.h"
#endif

namespace oceanbase
{
    namespace sql 
    {
        //------------------------------------Udf------------------------------------
        class pythonUdf {
        public:
            char* pycall;//code
            std::string name;
            int arg_count;//参数数量
            //参数类型
            PyUdfSchema::PyUdfArgType* arg_types;
            //返回值类型,多返回值?
            bool mul_rt;
            PyUdfSchema::PyUdfRetType* rt_type;
            //运行变量
            PyObject *pArgs, *pResult;
            //初始化变量
            PyObject *pModule, *pFunc, *pInitial, *dic, *v;
            //向量化参数
            int batch_size;
            
        public:
            pythonUdf();
            ~pythonUdf();
            std::string get_name();
            bool init_python_udf(std::string name, char* pycall, PyUdfSchema::PyUdfArgType* arg_list, int length, PyUdfSchema::PyUdfRetType* rt_type, int batch_size = 256 /* default */ );
            int get_arg_count();
            
            //设置参数->重载
            bool set_arg_at(int i, long const& arg);
            bool set_arg_at(int i, double const& arg);
            bool set_arg_at(int i, bool const& arg);
            bool set_arg_at(int i, std::string const& arg);
            bool set_arg_at(int i, PyObject* arg);

            //初始化及执行
            bool execute_initial();
            bool execute();
            //获取执行结果->重载
            bool get_result(long& result);
            bool get_result(double& result);
            bool get_result(bool& result);
            bool get_result(std::string& result);
            bool get_result(PyObject*& result);

            //异常处理
            void process_python_exception();
            void message_error_dialog_show(char *buf);

            double lastTime;
            //调整batch size -> Clipper AIMD
            void changeBatchAIMD(double time) {
                if(lastTime == 0) {
                    lastTime = time;
                    batch_size += 256;
                    return;
                } else {
                    if(time < lastTime * 0.9 && batch_size < 4096) { //消耗时间降低10%
                        lastTime = time;
                        batch_size += 256;
                    } else {  //终止逻辑
                        //batch_size = (1 - 0.1) * batch_size; //退避系数 0.1
                        return;
                    }
                }
            }
        };

        //------------------------------------Engine------------------------------------
        //单例模式
        class pythonUdfEngine {
        private:
            //key-value map,存储已经初始化的python_udf
            std::map<pid_t, pythonUdf*> udf_pool;
            //只能有一个实例存在
            static pythonUdfEngine *current_engine;
            double tu;
            
        public:
            pythonUdfEngine(/* args */);
            ~pythonUdfEngine();
            //懒汉模式
            static pythonUdfEngine* init_python_udf_engine(pid_t tid) {
                if(current_engine == NULL) {
                    struct timeval t1, t2, tsub;
                    //double tu;
                    gettimeofday(&t1, NULL);
                    current_engine = new pythonUdfEngine();
                    gettimeofday(&t2, NULL);
                    timersub(&t2, &t1, &tsub);
                    current_engine->tu = tsub.tv_sec*1000 + (1.0 * tsub.tv_usec) / 1000;
                }
                return current_engine;
            }
            bool insert_python_udf(pid_t, pythonUdf *udf);
            bool get_python_udf(pid_t name, pythonUdf *& udf);//获取udf，只能有一个同名实例存在
        };
    }
}