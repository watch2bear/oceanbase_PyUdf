#define USING_LOG_PREFIX SQL_ENG

#include "ob_python_udf_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

#include <iostream>
#include <fstream>

namespace oceanbase
{
using namespace common;
namespace sql
{

typedef share::schema::ObPythonUdfEnumType::PyUdfRetType PyUdfType;


static bool with_batch_control_ = true; // 是否进行batch size控制
static bool with_full_funcache_ = false; // 是否进行粗粒度缓存
static bool with_fine_funcache_ = false; // 是否进行细粒度缓存


OB_SERIALIZE_MEMBER((ObPythonUDFSpec, ObOpSpec),
                    udf_exprs_,
                    input_exprs_);

ObPythonUDFSpec::ObPythonUDFSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), udf_exprs_(alloc), input_exprs_(alloc) {}

ObPythonUDFSpec::~ObPythonUDFSpec() {}

ObPythonUDFOp::ObPythonUDFOp(
    ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), controller_()
{
  int ret = OB_SUCCESS;
  //predict_size_ = 256;
  //predict_size_ = MY_SPEC.max_batch_size_; //default

  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  //ObMemAttr attr(tenant_id, ObModIds::RESTORE, ObCtxIds::MEMSTORE_CTX_ID);
  //controller_.set_attr(attr);
  /*if (OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Init python udf store controller failed", K(ret));
  }*/
}

ObPythonUDFOp::~ObPythonUDFOp() {
  //controller_.free();
}

int ObPythonUDFOp::inner_open()
{
  int ret = OB_SUCCESS;

  //initialize Python Intepreter
  //Py_InitializeEx(!Py_IsInitialized());
  //_save = PyEval_SaveThread();

  // init context
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Init python udf store controller failed", K(ret));
  //} else if (OB_FAIL(init_udfs(MY_SPEC.udf_exprs_))) {
  //  ret = OB_INIT_FAIL;
  //  LOG_WARN("Import python udfs", K(ret));
  } else {
    // check attrs
    ret = ObOperator::inner_open();
  }
  //ret = ObOperator::inner_open();
  return ret;
}

int ObPythonUDFOp::inner_close()
{
  int ret = OB_SUCCESS;

  //PyEval_RestoreThread((PyThreadState *)_save);
  //Py_FinalizeEx(); // Python Intepreter

  // free attrs;
  if (OB_FAIL(controller_.free())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Free python udf store controller failed", K(ret));
  } else {
    ret = ObOperator::inner_close();
  }
  //ret = ObOperator::inner_close();
  return ret;
}

int ObPythonUDFOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  // reset attrs
  const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_FAIL(controller_.free()) || 
      OB_FAIL(controller_.init(MY_SPEC.max_batch_size_,
                               MY_SPEC.udf_exprs_, 
                               MY_SPEC.input_exprs_,
                               tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ObOperator::inner_rescan();
  }
  //ret = ObOperator::inner_rescan();
  return ret;
}

void ObPythonUDFOp::destroy()
{
  ObOperator::destroy();
}

int ObPythonUDFOp::inner_get_next_row() 
{
  int ret = OB_SUCCESS;
  // not support yet
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from child failed", K(ret));
    }
  } else {/* do nothing */}
  return ret;
}

/*使用缓存，确保每轮迭代能够以合适的批次大小计算Python UDF
* 1. 调用Python UDF expr的子expr
* 2. 复制frame上的输入至input缓存
* 3. 重复1，2至input缓存到达阈值
* 4. 直接在input缓存上计算Python UDF，并将结果保存至output缓存
* 5. 将output缓存复制回frame上的输出
* 6. 重复5至output缓存清空
 */
int ObPythonUDFOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  // get data from buffer
  
  struct timeval ut1, ut2, ut3, ut4, ut5, ut6, ut7, ut8, ut9, ut10, ut11;
  // Ensure GIL
  bool nStatus = PyGILState_Check();
  PyGILState_STATE gstate;
  if(!nStatus) {
    gstate = PyGILState_Ensure();
    nStatus = true;
  }
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  
  if (with_batch_control_) {
    if (OB_SUCC(ret) && !controller_.can_output()) {
      clear_evaluated_flag();
      controller_.resize(controller_.get_desirable() * 2);
      const ObBatchRows *child_brs = nullptr;
      gettimeofday(&ut10, NULL);
      while (OB_SUCC(ret) && !brs_.end_ && !controller_.is_full()) { // while loop直至缓存填满
        if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Get child next batch failed.", K(ret));
        } else if (brs_.copy(child_brs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Copy child batch rows failed.", K(ret));
        } else if (OB_FAIL(controller_.store(eval_ctx_, brs_))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Save input batchrows failed.", K(ret));
        }
      }
      gettimeofday(&ut11, NULL);
      if (with_full_funcache_||with_fine_funcache_){
        // 检查每个cell是否有缓存，如果有就直接将其标识出来
        gettimeofday(&ut1, NULL);
        controller_.check_cached_result_on_cells(eval_ctx_, controller_.get_desirable() * 2);
        gettimeofday(&ut2, NULL);
        if (OB_FAIL(ret) || OB_FAIL(controller_.process_with_cache(eval_ctx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        }
        gettimeofday(&ut3, NULL);
      }else{
        gettimeofday(&ut4, NULL);
        if (OB_FAIL(ret) || OB_FAIL(controller_.process())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        }
        gettimeofday(&ut5, NULL);
      }
    }
  } else { // 无batch size控制
    if (OB_SUCC(ret)) { 
      clear_evaluated_flag();
      controller_.resize(controller_.get_desirable());
      const ObBatchRows *child_brs = nullptr;
      if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get child next batch failed.", K(ret));
      } else if (brs_.copy(child_brs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Copy child batch rows failed.", K(ret));
      } else if (OB_FAIL(controller_.store(eval_ctx_, brs_))){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Save input batchrows failed.", K(ret));
      } 
      // else if (OB_FAIL(controller_.process())) {
      //   ret = OB_ERR_UNEXPECTED;
      //   LOG_WARN("Process python udf failed.", K(ret));
      // } else {}
      if (with_full_funcache_||with_fine_funcache_){
        // 检查每个cell是否有缓存，如果有就直接将其标识出来
        controller_.check_cached_result_on_cells(eval_ctx_, controller_.get_desirable());
        if (OB_FAIL(ret) || OB_FAIL(controller_.process_with_cache(eval_ctx_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        }
      }else{
        if (OB_FAIL(ret) || OB_FAIL(controller_.process())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Process python udf failed.", K(ret));
        }
      }
    }
  }
  
  if (with_full_funcache_||with_fine_funcache_){
    gettimeofday(&ut6, NULL);
    if (OB_FAIL(ret) || OB_FAIL(controller_.restore_with_cache(eval_ctx_, brs_, max_row_cnt))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Restore output batchrows failed.", K(ret));
    } else {
      // reset all spec filters
      FOREACH_CNT_X(e, MY_SPEC.filters_, OB_SUCC(ret)) {
        (*e)->clear_evaluated_flag(eval_ctx_);
        (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
      }
    }
    gettimeofday(&ut7, NULL);
  }else{
    gettimeofday(&ut8, NULL);
    if (OB_FAIL(ret) || OB_FAIL(controller_.restore(eval_ctx_, brs_, max_row_cnt))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Restore output batchrows failed.", K(ret));
    } else {
      // reset all spec filters
      FOREACH_CNT_X(e, MY_SPEC.filters_, OB_SUCC(ret)) {
        (*e)->clear_evaluated_flag(eval_ctx_);
        (*e)->get_eval_info(eval_ctx_).clear_evaluated_flag();
      }
    }
    gettimeofday(&ut9, NULL);
  }
  
  gettimeofday(&t2, NULL);
  double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec);
  double time_s = timeuse / 1000;
  // std::fstream time_log;
  // time_log.open("/home/test/experiments/oceanbase/opt/op_time.log", std::ios::app);
  // time_log << time_s << " ";
  // time_log.close(); 

  //PyGC_Enable();
  //PyGC_Collect();
  // Release GIL
  if(nStatus) {
    PyGILState_Release(gstate);
  }

  /*std::ofstream outputFile("/home/test/experiments/oceanbase/px/log/batch_size.txt", std::ios::app);
  if (outputFile) {
    outputFile << brs_.size_ << std::endl;
  }
  outputFile.close();*/
  if(controller_.can_output()){
    return ret;
  }
  std::string file_name("/home/");
  file_name.append(std::string("runlog"));
  file_name.append(".log");
  std::fstream f;
  f.open(file_name, std::ios::out | std::ios::app); // 追加写入
  f << "Start a new batch!" << std::endl;
  f << "execution time: " << timeuse/1000 << " ms" << std::endl;
  f << "get_next_batch time: " << (ut11.tv_sec - ut10.tv_sec) * 1000 + (double)(ut11.tv_usec - ut10.tv_usec) / 1000 << " ms" << std::endl;
  f << "check_cached_result_on_cells time: " << (ut2.tv_sec - ut1.tv_sec) * 1000 + (double)(ut2.tv_usec - ut1.tv_usec) / 1000 << " ms" << std::endl;
  f << "process_with_cache time: " << (ut3.tv_sec - ut2.tv_sec) * 1000 + (double)(ut3.tv_usec - ut2.tv_usec) / 1000 << " ms" << std::endl;
  f << "process time: " << (ut5.tv_sec - ut4.tv_sec) * 1000 + (double)(ut5.tv_usec - ut4.tv_usec) / 1000 << " ms" << std::endl;
  f << "restore_with_cache time: " << (ut7.tv_sec - ut6.tv_sec) * 1000 + (double)(ut7.tv_usec - ut6.tv_usec) / 1000 << " ms" << std::endl;
  f << "restore time: " << (ut9.tv_sec - ut8.tv_sec) * 1000 + (double)(ut9.tv_usec - ut8.tv_usec) / 1000 << " ms" << std::endl;
  return ret;
}

/* find predict_size of buffer */
int ObPythonUDFOp::find_predict_size(ObExpr *expr, int32_t &predict_size)
{
  int ret = OB_SUCCESS;
  /*int expr_size = 256; //default
  if (expr->type_ == T_FUN_PYTHON_UDF) {
    int size = static_cast<ObPythonUdfInfo *>(expr->extra_info_)->predict_size;
    expr_size = size > expr_size ? size : expr_size;
  } else {
    for (int32_t i = 0; i < expr->arg_cnt_; i++) {
      find_predict_size(expr->args_[i], expr_size);
    }
  }
  if (predict_size < expr_size)
    predict_size = expr_size;
  if (predict_size > max_buffer_size_)
    predict_size = max_buffer_size_;*/
  return ret;
}

int ObPythonUDFOp::clear_calc_exprs_evaluated_flags() 
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < MY_SPEC.calc_exprs_.count(); i++) {
    MY_SPEC.calc_exprs_[i]->get_eval_info(eval_ctx_).clear_evaluated_flag();
  }
  return ret;
}

/* ----------------------------------Init Python UDF------------------------------------- */

int ObPythonUDFOp::init_udfs(const common::ObIArray<ObExpr *> &udf_exprs)
{
  int ret = OB_SUCCESS;
  //Acquire GIL
  bool nStatus = PyGILState_Check();
  PyGILState_STATE gstate;
  if(!nStatus) {
    gstate = PyGILState_Ensure();
    nStatus = true;
  }
  // init all python udfs
  for (int i = 0; OB_SUCC(ret) && i < udf_exprs.count(); ++i) {
    ObExpr *udf_expr = udf_exprs.at(i);
    ObPythonUDFMeta udf_meta = static_cast<ObPythonUdfInfo *>(udf_expr->extra_info_)->udf_meta_;
    if (udf_meta.init_) {
      LOG_DEBUG("udf meta has already inited", K(ret));
    } else if (udf_meta.ret_ == ObPythonUdfEnumType::PyUdfRetType::UDF_UNINITIAL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf meta ret type is null", K(ret));
    } else if (udf_meta.pycall_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("udf meta pycall is null", K(ret));
    } else {
      // check udf param types
      for (int j = 0; OB_SUCC(ret) && j < udf_expr->arg_cnt_; ++j) {
        ObExpr *expr = udf_expr->args_[j];
        if (expr == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("python udf arg expr is null", K(ret));
        } else {
          switch(expr->datum_meta_.type_) {
            case ObCharType :
            case ObVarcharType :
            case ObTinyTextType :
            case ObTextType :
            case ObMediumTextType :
            case ObLongTextType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUdfEnumType::PyUdfRetType::STRING) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObTinyIntType :
            case ObSmallIntType :
            case ObMediumIntType :
            case ObInt32Type :
            case ObIntType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUdfEnumType::PyUdfRetType::INTEGER) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObDoubleType : 
              if(udf_meta.udf_attributes_types_.at(j) != ObPythonUdfEnumType::PyUdfRetType::REAL) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("the type of param is incorrect", K(ret), K(j));
              }
              break;
            case ObNumberType : 
                //decimal
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("not support decimal", K(ret), K(j));
                break;
            default : 
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("not support param type", K(ret));
          }
        }
      }
      // import python udf code
      if (OB_FAIL(ret) || OB_FAIL(import_udf(udf_meta))) {
        ret = OB_INIT_FAIL;
        LOG_WARN("Fail to import udf", K(ret));
      } else {
        udf_meta.init_ = true;
      }
    }
  }
  //release GIL
  if(nStatus)
    PyGILState_Release(gstate);
  return ret;
}

int ObPythonUDFOp::import_udf(const share::schema::ObPythonUDFMeta &udf_meta)
{
  int ret = OB_SUCCESS;

  //runtime variables
  PyObject *pModule = NULL;
  PyObject *dic = NULL;
  PyObject *v = NULL;
  PyObject *pInitial = NULL;

  //name
  std::string name(udf_meta.name_.ptr());
  name = name.substr(0, udf_meta.name_.length());
  std::string pyinitial_handler = name + "_pyinitial";
  std::string pyfun_handler = name + "_pyfun";
  //pycall
  std::string pycall(udf_meta.pycall_.ptr());
  pycall = pycall.substr(0, udf_meta.pycall_.length());
  pycall.replace(pycall.find("pyinitial"), 9, pyinitial_handler);
  pycall.replace(pycall.find("pyfun"), 5, pyfun_handler);
  const char* pycall_c = pycall.c_str();

  // prepare and import python code
  pModule = PyImport_AddModule("__main__"); // load main module
  if(pModule == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to import main module", K(ret));
  } else if ((dic = PyModule_GetDict(pModule)) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get main module dic", K(ret));
  } else if ((v = PyRun_StringFlags(pycall_c, Py_file_input, dic, dic, NULL)) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to write pycall into module", K(ret));
  } else if ((pInitial = PyObject_GetAttrString(pModule, pyinitial_handler.c_str())) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to import pyinitial", K(ret));
  } else if (PyObject_CallObject(pInitial, NULL) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to run pyinitial", K(ret));
  } else {
    LOG_DEBUG("Import python udf handler", K(ret));
  }
  return ret;
}

/* ---------------------------------- ObColInputStore -------------------------------------*/
int ObColInputStore::init(const common::ObIArray<ObExpr *> &exprs,
                          int64_t batch_size,
                          int64_t length)
{
  int ret = OB_SUCCESS;
  exprs_.init(exprs.count());
  datums_copy_.init(exprs.count());
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    ObDatum *datums_buf = nullptr;
    datums_buf = static_cast<ObDatum *>(buf_alloc_.alloc(sizeof(ObDatum) * length));
    if (datums_buf == nullptr) {
      //分配datums_buf空间
      ret = OB_INIT_FAIL;
      LOG_WARN("Memory allocation failed.", K(ret));
    } else if (OB_FAIL(exprs_.push_back(expr)) || OB_FAIL(datums_copy_.push_back(datums_buf))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init datums_copy and other exprs failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    length_ = length;
    batch_size_ = batch_size;
    saved_size_ = 0;
    output_idx_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObColInputStore::free()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  /*for (int i = 0; i < datums_copy_.count(); ++i) {
    ObDatum *datums_buf = datums_copy_.at(i);
    buf_alloc_.free(datums_buf);
  }*/
  return ret;
}

int ObColInputStore::reuse()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  saved_size_ = 0;
  output_idx_ = 0;
  return ret;
}

int ObColInputStore::reset(int64_t length)
{
  int ret = OB_SUCCESS;
  if (length <= length_) {
    reuse();
  } else if (OB_FAIL(free())) {
    // memory leak
    ret = OB_ERR_UNEXPECTED;
  } else {
    datums_copy_.reset();
    datums_copy_.init(exprs_.count());
    for (int i = 0; i < exprs_.count(); ++i) {
      ObExpr *e = exprs_.at(i);
      ObDatum *datums_buf = static_cast<ObDatum *>(buf_alloc_.alloc(length * sizeof(ObDatum)));
      datums_copy_.push_back(datums_buf);
    }
    length_ = length;
    saved_size_ = 0;
    output_idx_ = 0;
  }
  return ret;
}

int ObColInputStore::save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (saved_size_ + cnt > length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other Input Store overflow.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      expr->cast_to_uniform(brs.size_, eval_ctx); // 数据格式转换过程可能会产生额外开销
      ObDatum *src = expr->locate_batch_datums(eval_ctx);
      ObDatum *dst = datums_copy_.at(i);
      int j = 0, zero = 0;
      int *src_idx;
      if (expr->is_const_expr())
        src_idx = &zero;
      else
        src_idx = &j;
      int dst_idx = saved_size_; // use dst idx to reform data
      for (j = 0; OB_SUCC(ret) && j < brs.size_; ++j) {
        if (brs.skip_->at(j)) {
          /* do nothing */
        } else {
          dst[dst_idx++].deep_copy(src[*src_idx], tmp_alloc_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      saved_size_ += cnt;
    }
  }
  return ret;
}

int ObColInputStore::save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (saved_size_ + cnt > length_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Other Input Store overflow.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      ObIVector *vector = expr->get_vector(eval_ctx);
      ObDatum *dst = datums_copy_.at(i);
      int dst_idx = saved_size_; // use dst idx to reform data
      for (int64_t j = 0; OB_SUCC(ret) && j < brs.size_; ++j) {
        if (brs.skip_->at(j)) {
          /* do nothing */
        } else {
          // deep copy from vector to datums
          ObLength copy_len;
          const char *payload;
          vector->get_payload(j, payload, copy_len);
          char *buf = nullptr;
          buf = static_cast<char *>(tmp_alloc_.alloc(copy_len));
          if (buf == nullptr) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(copy_len), K(ret));
          } else {
            MEMCPY(buf, payload, copy_len);
            dst[dst_idx].ptr_ = buf;
            dst[dst_idx].len_ = copy_len;
            ++dst_idx;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      saved_size_ += cnt;
    }
  }
  return ret;
}

int ObColInputStore::load_batch(ObEvalCtx &eval_ctx, int64_t load_size)
{
  int ret = OB_SUCCESS;
  if (output_idx_ + load_size > saved_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not enough data to load.", K(ret));
  } else {
    for (int64_t i = 0; i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      ObDatum *src = datums_copy_.at(i);
      ObDatum *dst = expr->locate_batch_datums(eval_ctx);
      for (int j = 0; j < load_size; ++j) {
        dst[j].set_datum(src[output_idx_ + j]);
      }
    }
    if (OB_SUCC(ret)) {
      output_idx_ += load_size;
    }
  }
  return ret;
}

int ObColInputStore::load_vector(ObEvalCtx &eval_ctx, int64_t load_size)
{
  int ret = OB_SUCCESS;
  if (output_idx_ + load_size > saved_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not enough data to load.", K(ret));
  } else {
    for (int64_t i = 0; i < exprs_.count(); ++i) {
      ObExpr *expr = exprs_.at(i);
      expr->init_vector_for_write(eval_ctx, expr->get_default_res_format(), load_size);
      ObDatum *src = datums_copy_.at(i);
      ObIVector *dst = expr->get_vector(eval_ctx);
      for (int j = 0; j < load_size; ++j) {
        dst->set_payload(j, src[output_idx_ + j].ptr_, src[output_idx_ + j].len_);
      }
    }
    if (OB_SUCC(ret)) {
      output_idx_ += load_size;
    }
  }
  return ret;
}
/* ---------------------------------- ObPUInputStore -------------------------------- */
int ObPUInputStore::init(common::ObIAllocator *alloc, ObExpr *expr, int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Repeat allocation", K(ret));
  } else if (alloc == nullptr || expr == nullptr || length <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("Uninit allocator and expression.", K(ret));
  } else if (expr->type_ != T_FUN_PYTHON_UDF) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not Python UDF Expr Type.", K(ret));
  } else {
    buf_alloc_ = alloc;
    expr_ = expr;
    if (OB_FAIL(alloc_data_ptrs(length)) || data_ptrs_ == nullptr) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init ObPUInputStore failed.", K(ret), K(alloc), K(expr->arg_cnt_));
    } else {
      inited_ = true;
      saved_size_ = 0;
    }
  }
  return ret;
}

int ObPUInputStore::alloc_data_ptrs(int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_) ) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Repeat allocation.", K(ret));
  } else if (buf_alloc_ == nullptr || expr_ == nullptr || expr_->arg_cnt_ <= 0 || length <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("Uninit allocator and expression.", K(ret));
  } else {
    // allocated by buf allocator
    data_ptrs_ = static_cast<char **>(buf_alloc_->alloc(expr_->arg_cnt_ * sizeof(char *))); // data lists
    if (data_ptrs_ == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Allocate data_ptr_ memory failed", K(ret), K(buf_alloc_), K(expr_->arg_cnt_));
    }
    for (int i = 0; i < expr_->arg_cnt_ && OB_SUCC(ret); ++i) {
      ObExpr *e = expr_->args_[i];
      data_ptrs_[i] = nullptr;
      /* allocate by datum type */
      switch(e->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(ObDatum))); // string lists
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(int))); // int lists
          break;
        }
        case ObDoubleType: {
          data_ptrs_[i] = reinterpret_cast<char *>(buf_alloc_->alloc(length * sizeof(double))); // double lists
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input arg type, alloc_data_ptrs failed.", K(ret));
        }
      }
      if (data_ptrs_[i] == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Allocate data_ptr_[i] memory failed.", K(ret), K(buf_alloc_), K(length_), K(i), K(e->datum_meta_.type_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    length_ = length;
  }
  return ret;
}

int ObPUInputStore::reuse()
{
  int ret = OB_SUCCESS;
  tmp_alloc_.reset();
  saved_size_ = 0;
  return ret;
}

int ObPUInputStore::reset(int64_t length /* default = 0 */)
{
  int ret = OB_SUCCESS;
  if (length <= length_) {
    ret = reuse();
  } else if (OB_FAIL(free())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(alloc_data_ptrs(length))) {
    ret = OB_INIT_FAIL;
  } else {
    saved_size_ = 0;
    inited_ = true;
  }
  return ret;
}

int ObPUInputStore::free() {
  int ret = OB_SUCCESS;
  length_ = 0;
  //data_ptrs_ = nullptr;
  saved_size_ = 0;
  inited_ = false;
  tmp_alloc_.reset();
  return ret;
}

int ObPUInputStore::save_batch(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int brs_cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  if (expr_ == nullptr || expr_->arg_cnt_ <= 0) {
    // empty exprs_: do nothing
  } else if (data_ptrs_ == nullptr || !inited_) {
    ret = OB_NOT_INIT;
  } else if (saved_size_ + brs_cnt <= length_) {
    for (int64_t i = 0; i < expr_->arg_cnt_; i++) {
      ObExpr *e = expr_->args_[i];
      ObDatum *src = e->locate_batch_datums(eval_ctx);
      int j = 0, zero = 0;
      int64_t dst_idx = saved_size_;
      int *src_idx;
      if (!expr_->args_[i]->is_const_expr()) 
        src_idx = &j;
      else 
        src_idx = &zero;
      switch(e->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          ObDatum *dst = reinterpret_cast<ObDatum *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            if (!brs.skip_->at(j)) {
              //copy str
              ObDatum &src_datum = src[*src_idx];
              dst[dst_idx++].deep_copy(src_datum, tmp_alloc_);
            }
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          int *dst = reinterpret_cast<int *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            ObDatum &src_datum = src[*src_idx];
            if (!brs.skip_->at(j)) {
              // copy int
              dst[dst_idx++] = src_datum.get_int();
            }
          }
          break;
        }
        case ObDoubleType: {
          double *dst = reinterpret_cast<double *>(data_ptrs_[i]);
          for (j = 0; j < brs.size_; j++) {
            ObDatum &src_datum = src[*src_idx];
            if (!brs.skip_->at(j)) {
              // copy double
              dst[dst_idx++] = src_datum.get_double();
            }
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input arg type, save data failed in ObPUDataStore.", K(ret));
        }
      }
      if (data_ptrs_[i] == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Allocate data_ptr_[i] memory failed.", K(ret), K(tmp_alloc_), K(length_), K(i), K(e->datum_meta_.type_));
      }
    }
    saved_size_ += brs_cnt;
  } else {
    // op逻辑控制其不能超过阈值
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPUDataStore Stack Overflow.", K(ret));
  }
  return ret;
}

int ObPUInputStore::save_vector(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  // not implemented yet
  int ret = OB_NOT_SUPPORTED;
  UNUSED(eval_ctx);
  UNUSED(brs);
  return ret;
}

/* ----------------------------------- ObPythonUDFCell --------------------------------- */
int ObPythonUDFCell::init(ObExpr *expr, 
                          int64_t batch_size, 
                          int64_t length,
                          uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  /*const ObMemAttr mem_attr(tenant_id, "PythonUDF");
  // init python udf expr according to its metadata
  if (OB_FAIL(alloc_.init(ObMallocAllocator::get_instance(),
                          OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    ret = OB_NOT_INIT;
    LOG_WARN("Fail to init allocator", K(ret));
  } else */if (OB_FAIL(input_store_.init(&alloc_, expr, length))) {
    ret = OB_NOT_INIT;
    LOG_WARN("Init input store failed.", K(ret));
  } else {
    expr_ = expr;
    //desirable_ = expr->extra_info_.predict_size;
    ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
    desirable_ = info->predict_size; // 初始值256
    batch_size_ = batch_size;
    eval_type_ = info->udf_meta_.model_type_;
  }
  return ret;
}

int ObPythonUDFCell::free()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(input_store_.free())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Free Python UDF cell failed.", K(ret));
  } else {
    result_size_ = 0;
    result_store_ = nullptr; // possibly memory leak
    mid_result_store_ =nullptr;
    /*if (result_store_ != nullptr) {
      PyObject *result_store = reinterpret_cast<PyObject *>(result_store_);
      Py_CLEAR(result_store);
      result_store_ = nullptr;
    }*/
  }
  alloc_.reset();
  return ret;
}

int ObPythonUDFCell::do_store(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (expr_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("Expr in input store is NULL.", K(ret));
  } else {
    // decide save function according to data transfer type
    // 因为要判断skip并进行重整，不能直接MEMCPY，所以用save_batch方法就足够了
    ObBitVector &my_skip = expr_->get_pvt_skip(eval_ctx);
    for (int64_t i = 0; i < expr_->arg_cnt_ && OB_SUCC(ret); i++) {
      ObExpr *e = expr_->args_[i];
      // eval_batch() = eval_vector() + cast_to_uniform()
      //if (OB_FAIL(e->eval_batch(eval_ctx, *brs.skip_, brs.size_))) {
      //if (OB_FAIL(e->eval_batch(eval_ctx, my_skip, batch_size_))) {
      if (OB_FAIL(e->eval_vector(eval_ctx, brs))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Eval Python UDF args' vector/batch result failed.", K(ret));
      } else if (OB_FAIL(e->cast_to_uniform(brs.size_, eval_ctx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Cast to uniform failed.", K(ret));
      }
    }
    // expr参数结果已经被设置为UNIFORM格式
    if (OB_FAIL(ret) || OB_FAIL(input_store_.save_batch(eval_ctx, brs))) { 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Save Python UDF input batch failed.", K(ret), K(expr_), K(batch_size_));
    }
  }
  return ret;
}

// not used
int ObPythonUDFCell::do_process_all()
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  mid_result_store_=nullptr;
  result_size_ = 0;
  PyObject *pArgs = nullptr;
  int64_t eval_size;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  merged_udf_res_list.resize(info->udf_meta_.merged_udf_names_.count());
  for(int i=0;i<merged_udf_res_list.size();i++){
    merged_udf_res_list[i]=nullptr;
  }
  struct timeval t1, t2;
  //load numpy api
  _import_array();
  gettimeofday(&t1, NULL);
  if (OB_FAIL(wrap_input_numpy(pArgs, eval_size)) || pArgs == nullptr) { // wrap all input
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
  } else if (OB_FAIL(eval(pArgs, eval_size))) { // evaluation and keep the result
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Eval Python UDF failed.", K(ret));
  } else { /* do nothing */ }
  Py_CLEAR(pArgs);
  gettimeofday(&t2, NULL);
  double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
  double tps = eval_size * 1000000 / timeuse; // current tuples per sec
  double time_s = timeuse / 1000;
  // std::fstream time_log;
  // time_log.open("/home/test/experiments/oceanbase/opt/pf_time.log", std::ios::app);
  // time_log << time_s << " ";
  // time_log.close(); 
  // std::fstream tps_log;
  // tps_log.open("/home/test/experiments/oceanbase/opt/both_pps.log", std::ios::app);
  // tps_log << "batch size: " << eval_size << std::endl;
  // tps_log << "prediction processing speed: " << tps << std::endl;
  // tps_log.close();
  return ret;
}

int ObPythonUDFCell::do_process_all_with_cache(std::vector<bool>& bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  mid_result_store_=nullptr;
  result_size_ = 0;
  PyObject *pArgs = nullptr;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  merged_udf_res_list.resize(info->udf_meta_.merged_udf_names_.count());
  for(int i=0;i<merged_udf_res_list.size();i++){
    merged_udf_res_list[i]=nullptr;
  }
  struct timeval t1, t2;
  //load numpy api
  _import_array();
  gettimeofday(&t1, NULL);
  int64_t real_eval_size=0;
  if (OB_FAIL(wrap_input_numpy_with_cache(pArgs, 0, real_eval_size, input_store_.get_saved_size(), bit_vector, mid_res_bit_vector)) || pArgs == nullptr) { // wrap all input
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
  } else if (real_eval_size>0&&OB_FAIL(eval(pArgs, input_store_.get_saved_size()))) { // evaluation and keep the result
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Eval Python UDF failed.", K(ret));
  } else { /* do nothing */ }
  Py_CLEAR(pArgs);
  if(real_eval_size==0){
    result_size_ += input_store_.get_saved_size();
  }
  if (OB_SUCC(ret)) {
    input_store_.reuse();
  }
  gettimeofday(&t2, NULL);
  double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
  double tps = real_eval_size * 1000000 / timeuse; // current tuples per sec
  double time_s = timeuse / 1000;
  return ret;
}

// warp [idx, idx + predict_size_]
int ObPythonUDFCell::wrap_input_numpy_with_cache(PyObject *&pArgs, int64_t idx, 
  int64_t& real_eval_size, int64_t desirable_eval_size, std::vector<bool> &cached_bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  real_eval_size=desirable_eval_size;
  for(int i=idx; i<idx + desirable_eval_size; i++){
    if(cached_bit_vector[i]||mid_res_bit_vector[i])
      real_eval_size--;
  }
  if(real_eval_size==0){
    return ret;
  }
  // 将没有被缓存的元组组装，交给python环境执行
  pArgs = PyTuple_New(expr_->arg_cnt_); // malloc hook
  npy_intp elements[1] = {real_eval_size};
  if (expr_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("Expr in input store is nullptr.", K(ret));
  } else {
    for (int i = 0; i < expr_->arg_cnt_; ++i) {
      PyObject *numpyarray = nullptr;
      switch (expr_->args_[i]->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
          ObDatum *src = reinterpret_cast<ObDatum *>(input_store_.get_data_ptr_at(i)) + idx;
          int index=0;
          for (int j = 0; j < desirable_eval_size; ++j) {
            if(cached_bit_vector[idx+j]||mid_res_bit_vector[idx+j]){
              continue;
            }
            PyObject *unicode_str = PyUnicode_FromStringAndSize(src[j].ptr_, src[j].len_);
            PyArray_SETITEM((PyArrayObject *)numpyarray, 
              (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, index++), unicode_str);
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_INT32, NULL, NULL, 0, 0, NULL);
          int *src = reinterpret_cast<int *>(input_store_.get_data_ptr_at(i)) + idx;
          int index=0;
          for (int j = 0; j < desirable_eval_size; ++j) {
            if(cached_bit_vector[idx+j]||mid_res_bit_vector[idx+j]){
              continue;
            }
            PyArray_SETITEM((PyArrayObject *)numpyarray, 
              (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, index++), PyLong_FromLong(src[j]));
          }
          break;
        }
        case ObDoubleType: {
          numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_FLOAT64, NULL, NULL, 0, 0, NULL);
          double *src = reinterpret_cast<double *>(input_store_.get_data_ptr_at(i)) + idx;
          int index=0;
          for (int j = 0; j < desirable_eval_size; ++j) {
            if(cached_bit_vector[idx+j]||mid_res_bit_vector[idx+j]){
              continue;
            }
            PyArray_SETITEM((PyArrayObject *)numpyarray, 
              (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, index++), PyFloat_FromDouble(src[j]));
          }
          break;
        }
        default: {
          //error
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input type.", K(ret));
        }
      }
      if(PyTuple_SetItem(pArgs, i, numpyarray) != 0){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Set numpy array arg failed.", K(ret));
      }
    }
  }
  return ret;
}

int ObPythonUDFCell::do_process_with_mid_res_cache(int count_mid_res, int count_cols, std::vector<bool>& mid_res_bit_vector, std::vector<float*>& mid_res_vector, 
  std::vector<int>& cached_res_for_int, std::vector<double>& cached_res_for_double, std::vector<std::string>& cached_res_for_str){
  int ret = OB_SUCCESS;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  std::string name(info->udf_meta_.name_.ptr());
  auto ret_type=info->udf_meta_.ret_;
  name = name.substr(0, info->udf_meta_.name_.length());
  PyObject *pModule = NULL;
  pModule = PyImport_AddModule("__main__");
  std::string pyfun_handler_input = name+"_input_pyfun";
  PyObject *pFunc_input = PyObject_GetAttrString(pModule, pyfun_handler_input.c_str());
  PyObject *pArgs_input = PyTuple_New(1);
  PyObject *pResult_Array_input = NULL;
  PyObject *pResult_input = NULL;
  npy_intp numRows = count_mid_res;
  npy_intp numCols = count_cols;
  npy_intp dims[2] = {numRows, numCols};
  PyObject* pArray_input = PyArray_SimpleNew(2, dims, NPY_FLOAT32);
  float* data_input = static_cast<float*>(PyArray_DATA(reinterpret_cast<PyArrayObject*>(pArray_input)));
  int count=0;
  for(int i=0;i<mid_res_bit_vector.size();i++){
    if(mid_res_bit_vector[i]){
      std::copy(mid_res_vector[i], mid_res_vector[i]+numCols, data_input + count * numCols);
      count++;
    }
  }
  PyTuple_SetItem(pArgs_input, 0, pArray_input);
  pResult_Array_input = PyObject_CallObject(pFunc_input, pArgs_input);
  if (!pResult_Array_input) {
    ret = OB_ERR_UNEXPECTED;
  }
  bool isNumPyArray_input = PyArray_Check(pResult_Array_input);
  if(isNumPyArray_input)
    pResult_input = pResult_Array_input;
  else
    pResult_input = PyList_GetItem(pResult_Array_input, 0);
  count=0;
  for(int i=0;i<mid_res_bit_vector.size();i++){
    if(mid_res_bit_vector[i]){
      if(ret_type==PyUdfType::STRING){
        const char* value=PyUnicode_AsUTF8(
          PyArray_GETITEM((PyArrayObject *)pResult_input, (char *)PyArray_GETPTR1((PyArrayObject *)pResult_input, count++)));
        string value_str(value);
        cached_res_for_str[i]=value_str;
      }else if(ret_type==PyUdfType::INTEGER){
        int tmp=PyLong_AsLong(
          PyArray_GETITEM((PyArrayObject *)pResult_input, (char *)PyArray_GETPTR1((PyArrayObject *)pResult_input, count++)));
        cached_res_for_int[i]=tmp;
      }else if(ret_type==PyUdfType::REAL){
        double tmp=PyFloat_AsDouble(
          PyArray_GETITEM((PyArrayObject *)pResult_input, (char *)PyArray_GETPTR1((PyArrayObject *)pResult_input, count++)));
        cached_res_for_double[i]=tmp;
      }
    }
  }
  // Release Python objects
  Py_XDECREF(pFunc_input);
  //Py_XDECREF(pArgs_input);
  Py_XDECREF(pArray_input);
  Py_XDECREF(pResult_Array_input);
  return ret;
}

int ObPythonUDFCell::do_process_with_cache(std::vector<bool>& bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  mid_result_store_ = nullptr;
  result_size_ = 0;
  struct timeval t1, t2;
  struct timeval t3, t4;
  int64_t eval_size = 0;
  int desirable_eval_size=0;
  //load numpy api
  _import_array();
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  // bool batch_size_const = info->udf_meta_.batch_size_const_;
  merged_udf_res_list.resize(info->udf_meta_.merged_udf_names_.count());
  for(int i=0;i<merged_udf_res_list.size();i++){
    merged_udf_res_list[i]=nullptr;
  }
  gettimeofday(&t3, NULL);
  bool batch_size_const = false;
  for (int idx = 0; OB_SUCC(ret) && idx < input_store_.get_saved_size(); idx += desirable_eval_size) {
    gettimeofday(&t1, NULL);
    PyObject *pArgs = nullptr;
    int64_t saved_size = input_store_.get_saved_size();
    desirable_eval_size = (idx + desirable_) < saved_size ? desirable_ : saved_size - idx;
    int64_t real_eval_size=0;
    if (OB_FAIL(wrap_input_numpy_with_cache(pArgs, idx, real_eval_size, desirable_eval_size, bit_vector, mid_res_bit_vector)) ) { // wrap the input
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
    } else if (real_eval_size>0&&OB_FAIL(eval(pArgs, desirable_eval_size))) { // evaluation and keep the result
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Eval Python UDF failed.", K(ret));
    } else {
      if(real_eval_size==0){
        result_size_ += desirable_eval_size;
      }
      gettimeofday(&t2, NULL);
      ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
      if (!batch_size_const)
        modify_desirable(t1, t2, eval_size);
      double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
      double time_s = timeuse / 1000;
      Py_CLEAR(pArgs);
    }
    gettimeofday(&t4, NULL);
    double timeuse2 = (t4.tv_sec - t3.tv_sec) * 1000000 + (double)(t4.tv_usec - t3.tv_usec); // usec
    double time_s2 = timeuse2 / 1000;
  }
  return ret;
}

int ObPythonUDFCell::do_process()
{
  int ret = OB_SUCCESS;
  // pre process
  result_store_ = nullptr;
  mid_result_store_ = nullptr;
  result_size_ = 0;
  struct timeval t1, t2;
  struct timeval t3, t4;
  int64_t eval_size = 0;
  //load numpy api
  _import_array();
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  merged_udf_res_list.resize(info->udf_meta_.merged_udf_names_.count());
  for(int i=0;i<merged_udf_res_list.size();i++){
    merged_udf_res_list[i]=nullptr;
  }
  // bool batch_size_const = info->udf_meta_.batch_size_const_;
  gettimeofday(&t3, NULL);
  bool batch_size_const = false;
  for (int idx = 0; OB_SUCC(ret) && idx < input_store_.get_saved_size(); idx += eval_size) {
    gettimeofday(&t1, NULL);
    PyObject *pArgs = nullptr;
    if (OB_FAIL(wrap_input_numpy(pArgs, idx, desirable_, eval_size)) || pArgs == nullptr) { // wrap the input
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrap Cell Input Store as Python UDF input args failed.", K(ret));
    } else if (OB_FAIL(eval(pArgs, eval_size))) { // evaluation and keep the result
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Eval Python UDF failed.", K(ret));
    } else {
      gettimeofday(&t2, NULL);
      ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
      if (!batch_size_const)
        modify_desirable(t1, t2, eval_size);

      double timeuse = (t2.tv_sec - t1.tv_sec) * 1000000 + (double)(t2.tv_usec - t1.tv_usec); // usec
      double time_s = timeuse / 1000;
      // std::fstream time_log;
      // time_log.open("/home/test/experiments/oceanbase/opt/pf_time.log", std::ios::app);
      // time_log << time_s << " ";
      // time_log.close(); 
      Py_CLEAR(pArgs);
    }
    gettimeofday(&t4, NULL);
    double timeuse2 = (t4.tv_sec - t3.tv_sec) * 1000000 + (double)(t4.tv_usec - t3.tv_usec); // usec
    double time_s2 = timeuse2 / 1000;
    // std::fstream time_log2;
    // time_log2.open("/home/test/experiments/oceanbase/opt/process_time.log", std::ios::app);
    // time_log2 << time_s2 << " ";
    // time_log2.close(); 
  }
  return ret;
}

int ObPythonUDFCell::do_restore(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  if (output_idx + output_size > result_size_) { //确保访问结果数组时不会越界
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Store Overflow.", K(ret));
  } else if (expr_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr expr_.", K(ret));
  } else if (expr_->enable_rich_format() && OB_FAIL(do_restore_vector(eval_ctx, output_idx, output_size))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else if (!expr_->enable_rich_format() && OB_FAIL(do_restore_batch(eval_ctx, output_idx, output_size))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else {
    expr_->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObPythonUDFCell::do_restore_with_cache(bool can_use_cache, ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size, std::vector<double>& cached_res_for_double, std::vector<int>& cached_res_for_int,
  std::vector<std::string>& cached_res_for_str, std::vector<std::string>& input_list, std::vector<bool>& bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  if (output_idx + output_size > result_size_) { //确保访问结果数组时不会越界
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Result Store Overflow.", K(ret));
  } else if (expr_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected nullptr expr_.", K(ret));
  } else if (expr_->enable_rich_format() && OB_FAIL(do_restore_vector_with_cache(can_use_cache, eval_ctx, output_idx, output_size, cached_res_for_double, cached_res_for_int, cached_res_for_str, input_list, bit_vector, mid_res_bit_vector))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else if (!expr_->enable_rich_format() && OB_FAIL(do_restore_batch_with_cache(can_use_cache, eval_ctx, output_idx, output_size, cached_res_for_double, cached_res_for_int, cached_res_for_str, input_list, bit_vector, mid_res_bit_vector))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unsupported result type.", K(ret));
  } else {
    expr_->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObPythonUDFCell::do_restore_batch(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  ObDatum *result_datums = expr_->locate_batch_datums(eval_ctx);
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_string(common::ObString(PyUnicode_AsUTF8(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i)))));
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_int(PyLong_AsLong(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    case ObDoubleType: {
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        result_datums[i].set_double(PyFloat_AsDouble(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  return ret;
}

int ObPythonUDFCell::do_restore_batch_with_cache(bool can_use_cache, ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size, std::vector<double>& cached_res_for_double, std::vector<int>& cached_res_for_int,
  std::vector<std::string>& cached_res_for_str, std::vector<std::string>& input_list, std::vector<bool>& bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  ObSQLSessionInfo* session=eval_ctx.exec_ctx_.get_my_session();
  ObString udf_name=info->udf_meta_.name_;
  PyUDFCache& udf_cache=session->get_pyudf_cache();
  ObDatum *result_datums = expr_->locate_batch_datums(eval_ctx);
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  PyArrayObject *mid_result_store = reinterpret_cast<PyArrayObject *>(mid_result_store_);
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        if(can_use_cache&&bit_vector[i+output_idx]){
          result_datums[i].set_string(common::ObString(cached_res_for_str[i+output_idx].c_str()));
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          result_datums[i].set_string(common::ObString(cached_res_for_str[i+output_idx].c_str()));
          udf_cache.set_string(udf_name, input_list[i+output_idx], cached_res_for_str[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        const char* value=PyUnicode_AsUTF8(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        string value_str(value);
        //udf_cache.set_string(udf_name, input_list[i], value_str);
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            const char* tmpvalue = PyUnicode_AsUTF8(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            string tmpvalue_str(tmpvalue);
            udf_cache.set_string(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue_str);
          }
        }else{
          udf_cache.set_string(udf_name, input_list[i+output_idx], value_str);
        }
        result_datums[i].set_string(common::ObString(value));
        count++;
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        if(can_use_cache&&bit_vector[i+output_idx]){
          result_datums[i].set_int(cached_res_for_int[i+output_idx]);
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          result_datums[i].set_int(cached_res_for_int[i+output_idx]);
          udf_cache.set_int(udf_name, input_list[i+output_idx], cached_res_for_int[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        int value=PyLong_AsLong(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        //udf_cache.set_int(udf_name, input_list[i], value);
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            int tmpvalue=PyLong_AsLong(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            udf_cache.set_int(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue);
          }
        }else{
          udf_cache.set_int(udf_name, input_list[i+output_idx], value);
        }
        result_datums[i].set_int(value);
        count++;
      }
      break;
    }
    case ObDoubleType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        expr_->reset_ptr_in_datum(eval_ctx, i);
        if(can_use_cache&&bit_vector[i+output_idx]){
          result_datums[i].set_double(cached_res_for_double[i+output_idx]);
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          result_datums[i].set_double(cached_res_for_double[i+output_idx]);
          udf_cache.set_double(udf_name, input_list[i+output_idx], cached_res_for_double[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        double value=PyFloat_AsDouble(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        //udf_cache.set_double(udf_name, input_list[i], value);
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            double tmpvalue=PyFloat_AsDouble(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            udf_cache.set_double(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue);
          }
        }else{
          udf_cache.set_double(udf_name, input_list[i+output_idx], value);
        }
        result_datums[i].set_double(value);
        count++;
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  return ret;
}

int ObPythonUDFCell::do_restore_vector(ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size)
{
  int ret = OB_SUCCESS;
  //if (!expr_->get_eval_info(eval_ctx).evaluated_) {
  VectorFormat format = VEC_INVALID;
  const ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  switch(info->udf_meta_.ret_) {
    case PyUdfType::STRING:
      format = VEC_DISCRETE;
      //format = VEC_CONTINUOUS;
    break;
    case PyUdfType::INTEGER:
    case PyUdfType::REAL:
      format = VEC_FIXED;
    break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
  }
  expr_->init_vector_for_write(eval_ctx, format, batch_size_);
  ObIVector *vector = expr_->get_vector(eval_ctx);
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  // 构造vector并赋回expr_
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_string(i, common::ObString(PyUnicode_AsUTF8(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i)))));
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_int(i, PyLong_AsLong(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    case ObDoubleType: {
      for (int i = 0; i < output_size; ++i) {
        vector->set_double(i, PyFloat_AsDouble(
          PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + i))));
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  return ret;
}

int ObPythonUDFCell::do_restore_vector_with_cache(bool can_use_cache, ObEvalCtx &eval_ctx, int64_t output_idx, int64_t output_size, std::vector<double>& cached_res_for_double, std::vector<int>& cached_res_for_int,
  std::vector<std::string>& cached_res_for_str, std::vector<std::string>& input_list, std::vector<bool>& bit_vector, std::vector<bool>& mid_res_bit_vector)
{
  int ret = OB_SUCCESS;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  ObSQLSessionInfo* session=eval_ctx.exec_ctx_.get_my_session();
  ObString udf_name=info->udf_meta_.name_;
  PyUDFCache& udf_cache=session->get_pyudf_cache();
  //if (!expr_->get_eval_info(eval_ctx).evaluated_) {
  VectorFormat format = VEC_INVALID;
  switch(info->udf_meta_.ret_) {
    case PyUdfType::STRING:
      format = VEC_DISCRETE;
      //format = VEC_CONTINUOUS;
    break;
    case PyUdfType::INTEGER:
    case PyUdfType::REAL:
      format = VEC_FIXED;
    break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
  }
  expr_->init_vector_for_write(eval_ctx, format, batch_size_);
  ObIVector *vector = expr_->get_vector(eval_ctx);
  PyArrayObject *result_store = reinterpret_cast<PyArrayObject *>(result_store_);
  PyArrayObject *mid_result_store = reinterpret_cast<PyArrayObject *>(mid_result_store_);
  // 构造vector并赋回expr_
  switch(expr_->datum_meta_.type_) {
    case ObCharType:
    case ObVarcharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        if(can_use_cache&&bit_vector[i+output_idx]){
          vector->set_string(i, common::ObString(cached_res_for_str[i+output_idx].c_str()));
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          vector->set_string(i, common::ObString(cached_res_for_str[i+output_idx].c_str()));
          udf_cache.set_string(udf_name, input_list[i+output_idx], cached_res_for_str[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        const char* value=PyUnicode_AsUTF8(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        string value_str(value);
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            const char* tmpvalue = PyUnicode_AsUTF8(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            string tmpvalue_str(tmpvalue);
            udf_cache.set_string(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue_str);
          }
        }else{
          udf_cache.set_string(udf_name, input_list[i+output_idx], value_str);
        }
        vector->set_string(i, common::ObString(value));
        count++;
      }
      break;
    }
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        if(can_use_cache&&bit_vector[i+output_idx]){
          vector->set_int(i, cached_res_for_int[i+output_idx]);
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          vector->set_int(i, cached_res_for_int[i+output_idx]);
          udf_cache.set_int(udf_name, input_list[i+output_idx], cached_res_for_int[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        int value=PyLong_AsLong(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            int tmpvalue=PyLong_AsLong(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            if(OB_FAIL(udf_cache.set_int(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue))){
              ObString tmp2=info->udf_meta_.merged_udf_names_[j];
              const char * tmp33=input_list[i+output_idx].c_str();
              if(ret==OB_HASH_EXIST){
                ret=OB_SUCCESS;
              }else{
                LOG_WARN("set_int fail.", K(ret));
              }
            }
          }
        }else{
          udf_cache.set_int(udf_name, input_list[i+output_idx], value);
        }
        vector->set_int(i, value);
        count++;
      }
      break;
    }
    case ObDoubleType: {
      int count=0;
      for (int i = 0; i < output_size; ++i) {
        if(can_use_cache&&bit_vector[i+output_idx]){
          vector->set_double(i, cached_res_for_double[i+output_idx]);
          continue;
        }
        if(can_use_cache&&mid_res_bit_vector[i+output_idx]){
          vector->set_double(i, cached_res_for_double[i+output_idx]);
          udf_cache.set_double(udf_name, input_list[i+output_idx], cached_res_for_double[i+output_idx]);
          continue;
        }
        // 存储中间结果
        if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
          npy_intp numCols = PyArray_DIM(mid_result_store, 1);
          float* rowData = reinterpret_cast<float*>(PyArray_GETPTR2(mid_result_store, output_idx + count, 0));
          float* tmp_mid_result = new float[numCols];
          std::copy(rowData, rowData + numCols, tmp_mid_result);
          udf_cache.set_mid_result(info->udf_meta_.model_path_, input_list[i+output_idx], tmp_mid_result);
          udf_cache.mid_res_col_count_map[std::string(info->udf_meta_.model_path_.ptr(), info->udf_meta_.model_path_.length())]=numCols;
        }
        // 存入缓存
        double value=PyFloat_AsDouble(PyArray_GETITEM(result_store, (char *)PyArray_GETPTR1(result_store, output_idx + count)));
        if(info->udf_meta_.ismerged_){
          for(int j=0; j<info->udf_meta_.merged_udf_names_.count(); j++){
            PyArrayObject *merged_udf_res = reinterpret_cast<PyArrayObject *>(merged_udf_res_list[j]);
            double tmpvalue=PyFloat_AsDouble(PyArray_GETITEM(merged_udf_res, (char *)PyArray_GETPTR1(merged_udf_res, output_idx + count)));
            udf_cache.set_double(info->udf_meta_.merged_udf_names_[j], input_list[i+output_idx], tmpvalue);
          }
        }else{
          udf_cache.set_double(udf_name, input_list[i+output_idx], value);
        }
        vector->set_double(i, value);
        count++;
      }
      break;
    }
    default: {
      //error
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unsupported result type.", K(ret));
    }
  }
  return ret;
}

// warp all saved input
int ObPythonUDFCell::wrap_input_numpy(PyObject *&pArgs, int64_t &eval_size)
{
  return wrap_input_numpy(pArgs, 0, input_store_.get_saved_size(), eval_size);
}

// warp [idx, idx + predict_size_]
int ObPythonUDFCell::wrap_input_numpy(PyObject *&pArgs, int64_t idx, int64_t predict_size, int64_t &eval_size)
{
  int ret = OB_SUCCESS;
  pArgs = PyList_New(expr_->arg_cnt_); // malloc hook
  int64_t saved_size = input_store_.get_saved_size();
  eval_size = (idx + predict_size) < saved_size ? predict_size : saved_size - idx;
  npy_intp elements[1] = {eval_size};
  if (expr_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("Expr in input store is nullptr.", K(ret));
  } else {
    for (int i = 0; i < expr_->arg_cnt_; ++i) {
      PyObject *numpyarray = nullptr;
      switch (expr_->args_[i]->datum_meta_.type_) {
        case ObCharType:
        case ObVarcharType:
        case ObTinyTextType:
        case ObTextType:
        case ObMediumTextType:
        case ObLongTextType: {
          /*numpyarray = PyArray_New(&PyArray_Type, 
                                  1, 
                                  elements, 
                                  NPY_OBJECT, 
                                  NULL, 
                                  reinterpret_cast<PyObject **>(input_store_.get_data_ptr_at(i)), 
                                  eval_size, 
                                  0, 
                                  NULL);*/
          numpyarray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT, NULL, NULL, 0, 0, NULL);
          //PyObject **unicode_strs = reinterpret_cast<PyObject **>(input_store_.get_data_ptr_at(i)) + idx;
          // construct unicode str
          ObDatum *src = reinterpret_cast<ObDatum *>(input_store_.get_data_ptr_at(i)) + idx;
          for (int j = 0; j < eval_size; ++j) {
            PyObject *unicode_str = PyUnicode_FromStringAndSize(src[j].ptr_, src[j].len_);
            //put unicode string pyobject into numpy array
            //PyArray_SETITEM((PyArrayObject *)numpyarray, (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j), 
            //  unicode_strs[j]);
            PyArray_SETITEM((PyArrayObject *)numpyarray, 
              (char *)PyArray_GETPTR1((PyArrayObject *)numpyarray, j), unicode_str);
            
          }
          break;
        }
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          numpyarray = PyArray_New(&PyArray_Type, 
                                  1, 
                                  elements, 
                                  NPY_INT32, 
                                  NULL, 
                                  reinterpret_cast<int *>(input_store_.get_data_ptr_at(i)) + idx,  
                                  eval_size, 
                                  0, 
                                  NULL);
          break;
        }
        case ObDoubleType: {
          numpyarray = PyArray_New(&PyArray_Type, 
                                  1, 
                                  elements, 
                                  NPY_FLOAT64, 
                                  NULL, 
                                  reinterpret_cast<double *>(input_store_.get_data_ptr_at(i)) + idx, 
                                  eval_size, 
                                  0, 
                                  NULL);
          break;
        }
        default: {
          //error
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("Unsupported input type.", K(ret));
        }
      }
      if(PyList_SetItem(pArgs, i, numpyarray) != 0){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Set numpy array arg failed.", K(ret));
      }
    }
  }
  return ret;
}

int ObPythonUDFCell::eval(PyObject *pArgs, int64_t eval_size) {
  int ret = OB_SUCCESS;
  if (eval_size <= 0) {
    LOG_WARN("empty evaluation size", K(ret));
  } else {
    switch (eval_type_) {
    case ObPythonUdfEnumType::PyUdfUsingType::MODEL_SPECIFIC:
      if (OB_FAIL(eval_model_udf(pArgs, eval_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eval model specific udf failed", K(ret));
      }
      break;
    case ObPythonUdfEnumType::PyUdfUsingType::ARBITRARY_CODE:
      if (OB_FAIL(eval_python_udf(pArgs, eval_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eval arbitrary code python udf failed", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid udf model type", K(ret));
    }
  }
  return ret;
}

int ObPythonUDFCell::eval_python_udf(PyObject *pArgs, int64_t eval_size)
{
  int ret = OB_SUCCESS;
  // evalatuion in python interpreter

  // extract pyfun handler
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  std::string name(info->udf_meta_.name_.ptr());
  name = name.substr(0, info->udf_meta_.name_.length());
  std::string pyfun_handler = name+"_pyfun";
  std::string pyfun_handler_output = name+"_output_pyfun";

  PyObject *pModule = nullptr;
  PyObject *pFunc = nullptr;
  PyObject *pResult = nullptr;
  PyObject *resultArray = nullptr;

  if ((pModule = PyImport_AddModule("__main__")) == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Import main module failed.", K(ret));
  } else if ((pFunc = PyObject_GetAttrString(pModule, pyfun_handler.c_str())) == nullptr ||
              !PyCallable_Check(pFunc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get function handler failed.", K(ret));
  } else if (with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_&&
    ((pFunc = PyObject_GetAttrString(pModule, pyfun_handler_output.c_str())) == nullptr 
      || !PyCallable_Check(pFunc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get function handler failed.", K(ret));
  // } else if ((resultArray = PyObject_CallObject(pFunc, pArgs)) == nullptr) {
  } else if ((resultArray = PyObject_CallFunction(pFunc, "N", pArgs)) == nullptr) {
    ObExprPythonUdf::process_python_exception();
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Execute Python UDF error.", K(ret));
  } else {
    // 需要判断resultArray的返回类型，如果resultArray不是一个numpy数组，则代表他是一个多维数组，数组的第一维是正常执行流程的返回结果
    // 后面的每一维是导出的中间结果
    int isNumPyArray=PyArray_Check(resultArray);
    if(isNumPyArray){
      pResult = resultArray;
    }else{
      pResult = PyList_GetItem(resultArray, 0);
      ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
      if(with_full_funcache_&&info->udf_meta_.ismerged_){
        // 导出查询内冗余消除后的中间结果
        for(int i=0; i<merged_udf_res_list.size(); i++){
          PyObject *tmpArray = PyList_GetItem(resultArray, i+1);
          if (tmpArray==nullptr){
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("save merged udf res failed.", K(ret));
          }
          if (merged_udf_res_list[i] == nullptr) {
            merged_udf_res_list[i] = reinterpret_cast<void *>(tmpArray);
          } else {
            PyObject *concat = PyTuple_New(2);
            PyTuple_SetItem(concat, 0, reinterpret_cast<PyObject *>(merged_udf_res_list[i]));
            PyTuple_SetItem(concat, 1, tmpArray);
            merged_udf_res_list[i] = reinterpret_cast<void *>(PyArray_Concatenate(concat, 0));
          }
        }
      }
      if(with_fine_funcache_&&info->udf_meta_.has_new_output_model_path_){
        // 导出可复用的中间结果
        PyObject *tmpArray=PyList_GetItem(resultArray, PyList_Size(resultArray) - 1);
        if(mid_result_store_==nullptr){
          mid_result_store_=reinterpret_cast<void *>(tmpArray);
        } else{
          PyObject *concat = PyTuple_New(2);
          PyTuple_SetItem(concat, 0, reinterpret_cast<PyObject *>(mid_result_store_));
          PyTuple_SetItem(concat, 1, tmpArray);
          mid_result_store_ = reinterpret_cast<void *>(PyArray_Concatenate(concat, 0));
        }
      }
    }
    // numpy array concat
    if (result_store_ == nullptr) {
      result_store_ = reinterpret_cast<void *>(pResult);
    } else {
      PyObject *concat = PyTuple_New(2);
      PyTuple_SetItem(concat, 0, reinterpret_cast<PyObject *>(result_store_));
      PyTuple_SetItem(concat, 1, pResult);
      result_store_ = reinterpret_cast<void *>(PyArray_Concatenate(concat, 0));
    }
    result_size_ += eval_size;
  }
  return ret;
}

int ObPythonUDFCell::eval_model_udf(PyObject *pArgs, int64_t eval_size)
{
  int ret = OB_SUCCESS;
  // evalatuion of pyInstance

  // extract pyfun handler
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  std::string name(info->udf_meta_.name_.ptr());
  name = name.substr(0, info->udf_meta_.name_.length());
  //std::string pyfun_handler = name.append("_pyfun");
  std::string pyfun_handler("pyfun");

  std::string class_name = name + "UdfClass";
  std::string instance_name = name + "UdfInstance";
  PyObject *method_name = Py_BuildValue("s", "pyfun");

  ObUdfModelMeta model_meta = info->udf_meta_.udf_model_meta_.at(0);

  PyObject *pModule = nullptr;
  PyObject *pClass = nullptr;
  PyObject *pInstance = nullptr;
  PyObject *pFunc = nullptr;
  PyObject *pResult = nullptr;
  PyObject *pArgNames = nullptr;

  ObSEArray<ObString, 16L> &udf_attributes_names = info->udf_meta_.udf_attributes_names_;
  if (udf_attributes_names.count() != expr_->arg_cnt_) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Unexpected udf arg counts", K(ret));
  } else {
    pArgNames = PyList_New(expr_->arg_cnt_);
    for (int i = 0; OB_SUCC(ret) && i < expr_->arg_cnt_; ++i) {
      if (udf_attributes_names.at(i).ptr() == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Null udf atttrbutes names", K(ret));
      } else {
        std::string name = std::string(udf_attributes_names.at(i).ptr(), udf_attributes_names.at(i).length());
        const char* c_name = name.c_str();
        PyList_SetItem(pArgNames, i, Py_BuildValue("s", c_name));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // do evaluation in python interpreter
    if ((pModule = PyImport_AddModule("__main__")) == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Import main module failed.", K(ret));
    } else if ((pInstance = PyObject_GetAttrString(pModule, instance_name.c_str())) == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Get custom model udf instance failed.", K(ret));
    } else if ((pClass = PyObject_GetAttrString(pModule, class_name.c_str())) == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get custom model udf class", K(ret));
    } else if ((pFunc = PyObject_GetAttrString(pClass, pyfun_handler.c_str())) == nullptr ||
                !PyCallable_Check(pFunc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Check function handler failed.", K(ret));
    } else if ((pResult = PyObject_CallMethod(pInstance, "pyfun", "NN", pArgNames, pArgs)) == nullptr) {
      ObExprPythonUdf::process_python_exception();
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Execute Python model UDF error.", K(ret));
    } else {
      // numpy array concat
      if (result_store_ == nullptr) {
        result_store_ = reinterpret_cast<void *>(pResult);
      } else {
        PyObject *concat = PyTuple_New(2);
        PyTuple_SetItem(concat, 0, reinterpret_cast<PyObject *>(result_store_));
        PyTuple_SetItem(concat, 1, pResult);
        result_store_ = reinterpret_cast<void *>(PyArray_Concatenate(concat, 0));
      }
      result_size_ += eval_size;
    }
  }
  if (result_store_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null result store.", K(ret));
  }
  return ret;
}

int ObPythonUDFCell::modify_desirable(timeval &start, timeval &end, int64_t eval_size)
{
  int ret = OB_SUCCESS;
  ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(expr_->extra_info_);
  double timeuse = (end.tv_sec - start.tv_sec) * 1000000 + (double)(end.tv_usec - start.tv_usec); // usec
  double tps = eval_size * 1000000 / timeuse; // current tuples per sec
  // std::fstream tps_log;
  // tps_log.open("/home/test/experiments/oceanbase/opt/both_pps.log", std::ios::app);
  // tps_log << "batch size: " << eval_size << std::endl;
  // tps_log << "prediction processing speed: " << tps << std::endl;
  // tps_log.close();
  if (info->tps_s == 0) { // 初始化
    info->tps_s = tps;
    info->predict_size += info->delta; // 尝试调整
  } else if (info->round > info->round_limit || eval_size != info->predict_size) { //超过轮次，停止调整batch size 或 不符合predict size
    // do nothing
  } else if (tps > info->tps_s) { 
    // 提升阈值λ为10% 且 目前计算数量与给定batch size相符，进行调整
    if (tps < (1 + info->lambda) * info->tps_s)
      info->round++;
    info->tps_s = tps;
    info->predict_size += info->delta;
  } else { //tps <= info->tps_s
    // 未达到阈值
    info->tps_s = (1 - info->alpha) * info->tps_s + info->alpha * tps; // 平滑系数α
    if (tps > (1 - info->lambda) * info->tps_s)
      info->round++;
  }
  if (OB_SUCC(ret)) {
    desirable_ = info->predict_size;
  }
  return ret;
}

/* ----------------------------------- ObPUStoreController --------------------------------- */
int ObPUStoreController::init(int64_t batch_size,
                              const common::ObIArray<ObExpr *> &udf_exprs, 
                              const common::ObIArray<ObExpr *> &input_exprs,
                              const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // init python udf cells
  for (int i = 0; i < udf_exprs.count() && OB_SUCC(ret); ++i) {
    ObExpr *udf_expr = udf_exprs.at(i);
    void *cell_ptr = static_cast<ObPythonUDFCell *>(alloc_.alloc(sizeof(ObPythonUDFCell)));
    ObPythonUDFCell *cell = new (cell_ptr) ObPythonUDFCell();
    if (cell == nullptr || OB_FAIL(cell->init(udf_expr, batch_size, capacity_, tenant_id)) ) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init Python UDF Cell failed.", K(ret));
    } else {
      cells_list_.add_last(cell);
    }
  }
  // 初始化缓存相关变量
  cells_can_use_cache.resize(cells_list_.get_size());
  cells_cached_res_for_int.resize(cells_list_.get_size());
  cells_cached_res_for_double.resize(cells_list_.get_size());
  cells_cached_res_for_mid_result.resize(cells_list_.get_size());
  cells_cached_mid_res_bit_vector.resize(cells_list_.get_size());
  cells_cached_res_for_str.resize(cells_list_.get_size());
  input_list_for_cells.resize(cells_list_.get_size());
  cells_cached_res_bit_vector.resize(cells_list_.get_size());
  count_cached_mid_res=0;
  mid_res_cols_count=0;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(other_store_.init(input_exprs, batch_size, capacity_))) {
      ret = OB_INIT_FAIL;
      LOG_WARN("Init other input store failed.", K(ret));
    } else {
      stored_input_cnt_ = 0;
      stored_output_cnt_ = 0;
      output_idx_ = 0;
      batch_size_ = batch_size;
      tenant_id_ = tenant_id;
    }
  }
  return ret;
}

int ObPUStoreController::free()
{
  int ret = OB_SUCCESS;
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
       cell != header && OB_SUCC(ret); 
       cell = cell->get_next()) {
    if (OB_FAIL(cell->free())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Free Python UDF Cell failed.", K(ret));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(other_store_.free())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Free other input store failed.", K(ret));
    } else {
      // reset cnt & idx
      stored_input_cnt_ = 0;
      stored_output_cnt_ = 0;
      output_idx_ = 0;
    }
  }
  return ret;
}

int ObPUStoreController::store(ObEvalCtx &eval_ctx, ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  // do cells input store
  int64_t cnt = brs.size_ - brs.skip_->accumulate_bit_cnt(brs.size_);
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
       cell != header && OB_SUCC(ret); 
       cell = cell->get_next()) {
    if (OB_FAIL(cell->do_store(eval_ctx, brs))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do Python UDF Cell Store failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // do other inputs store
    // 要判断skip进行数据重整，使用local allocator进行数据复制
    // load时要转为相应类型的vector
    // 下层传上来的数据不一定为uniform格式，需要save_vector()或cast_to_uniform()
    if (OB_FAIL(other_store_.save_vector(eval_ctx, brs))) {
    //if (OB_FAIL(other_store_.save_batch(eval_ctx, brs))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Save other input cols failed.", K(ret));
    } else {
      stored_input_cnt_ += cnt;
    }
  }
  return ret;
}

int ObPUStoreController::process_with_cache(ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    /* do nothing */
  } else {
    int count=0;
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      if (cell->get_store_size() != stored_input_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unsaved input rows.", K(ret));
      } else if(with_batch_control_ && !with_full_funcache_&& OB_FAIL(cell->do_process())){ // without funcache
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process failed.", K(ret));
      } else if(with_batch_control_ && with_full_funcache_&& cells_can_use_cache[count]>0 && OB_FAIL(cell->do_process_with_cache(cells_cached_res_bit_vector[count], cells_cached_mid_res_bit_vector[count]))){ // with funcache
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process with funcache failed.", K(ret));
      } else if(with_batch_control_ && with_full_funcache_&& cells_can_use_cache[count]==0 && OB_FAIL(cell->do_process())){ // without funcache
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process with funcache failed.", K(ret));
      } else if (!with_batch_control_ && !with_full_funcache_ && OB_FAIL(cell->do_process_all())) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (!with_batch_control_ && with_full_funcache_ && cells_can_use_cache[count]==0 && OB_FAIL(cell->do_process_all())) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (!with_batch_control_ && with_full_funcache_ && cells_can_use_cache[count]>0 && OB_FAIL(cell->do_process_all_with_cache(cells_cached_res_bit_vector[count], cells_cached_mid_res_bit_vector[count]))) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (cell->get_result_size() != stored_input_cnt_){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unprocessed input rows.", K(ret));
      } else {
        // 使用中间结果作为输入计算udf结果
        if(with_fine_funcache_&&count_cached_mid_res>0){
          cell->do_process_with_mid_res_cache(count_cached_mid_res, mid_res_cols_count, cells_cached_mid_res_bit_vector[count], 
            cells_cached_res_for_mid_result[count], cells_cached_res_for_int[count], cells_cached_res_for_double[count],
            cells_cached_res_for_str[count]);
        }
        cell->reset_input_store();
      }
    }
    if (OB_SUCC(ret)) {
      stored_output_cnt_ = stored_input_cnt_;
      stored_input_cnt_ = 0;
      output_idx_ = 0;
    }
    count++;
  }
  return ret;
}

int ObPUStoreController::check_cached_result_on_cells(ObEvalCtx &eval_ctx, int size){
  int ret = OB_SUCCESS;
  int count=0;
  ObSQLSessionInfo* session=eval_ctx.exec_ctx_.get_my_session(); 
  PyUDFCache& udf_cache=session->get_pyudf_cache();
  ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
          // 初始化controller中的缓存相关数据结构
          ObPythonUdfInfo *info = static_cast<ObPythonUdfInfo *>(cell->get_expr()->extra_info_);
          ObString udf_name=info->udf_meta_.name_;
          auto ret_type=info->udf_meta_.ret_;
          input_list_for_cells[count]=std::vector<std::string>(size);
          cells_cached_res_bit_vector[count].resize(size);
          
          // 初始化细粒度冗余策略相关数据结构
          bool has_new_input_model_path=info->udf_meta_.has_new_input_model_path_;
          bool has_new_output_model_path=info->udf_meta_.has_new_output_model_path_;
          ObString model_path=info->udf_meta_.model_path_;
          bool can_be_used_redundent_cache_map_is_found=false;
          ObString can_be_used_model_path=info->udf_meta_.can_be_used_model_path_;
          if(with_fine_funcache_){
            cells_cached_res_for_mid_result[count].resize(size);
            cells_cached_mid_res_bit_vector[count].resize(size);
            if(has_new_output_model_path){
              // 如果has_new_output_model_path，就检索和建立与当前model path相应的map
              if(!udf_cache.find_fine_cache_for_model_path(model_path)){
                udf_cache.create_cache_for_model_path(model_path);
                info->is_new_mid_cache=true;
              }
            }
            if(has_new_input_model_path){
              // 如果has_new_input_model_path，就检索和建立与要复用的model path相应的map
              if(udf_cache.find_fine_cache_for_model_path(can_be_used_model_path)){
                can_be_used_redundent_cache_map_is_found=true; 
                mid_res_cols_count=udf_cache.mid_res_col_count_map[std::string(can_be_used_model_path.ptr(), can_be_used_model_path.length())];
              }else{
                can_be_used_redundent_cache_map_is_found=false;
              }
            }
          }

          // 构造input数组
          int input_count;
          if(info->udf_meta_.ismerged_)
            input_count=info->udf_meta_.origin_input_count_;
          else
            input_count=cell->get_expr()->arg_cnt_;
          for(int i=0; i<input_count; i++){
            switch (cell->get_expr()->args_[i]->datum_meta_.type_) {
                case ObCharType:
                case ObVarcharType:
                case ObTinyTextType:
                case ObTextType:
                case ObMediumTextType:
                case ObLongTextType: {
                  ObDatum *src = reinterpret_cast<ObDatum *>(cell->get_input_store().get_data_ptr_at(0));
                  for (int j=0; j < cell->get_store_size(); ++j) {
                    input_list_for_cells[count][j].append(std::string(src[j].ptr_, src[j].len_));
                  }
                  break;
                }
                case ObTinyIntType:
                case ObSmallIntType:
                case ObMediumIntType:
                case ObInt32Type:
                case ObIntType: { 
                  for (int j=0; j < cell->get_store_size(); ++j) {
                    input_list_for_cells[count][j].append(std::to_string(*(reinterpret_cast<int *>(cell->get_input_store().get_data_ptr_at(0))+ j)));
                  }
                  break;
                }
                case ObDoubleType: {
                  for (int j=0; j < cell->get_store_size(); ++j) {
                    input_list_for_cells[count][j].append(std::to_string(*(reinterpret_cast<double *>(cell->get_input_store().get_data_ptr_at(0))+ j)));
                  }
                  break;
                }
                default: {
                  //error
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("Unsupported input type.", K(ret));
                }
              }
          }
          if(ret_type==PyUdfType::STRING){
            cells_cached_res_for_str[count].resize(size);
          }else if(ret_type==PyUdfType::INTEGER){
            cells_cached_res_for_int[count].resize(size);
          }else if(ret_type==PyUdfType::REAL){
            cells_cached_res_for_double[count].resize(size);
          }
          //检查input是否有初始化缓存
          if(!info->is_new_full_cache&&udf_cache.find_cache_for_cell(udf_name)){
            // 有初始化缓存
            cells_can_use_cache[count]=0;
            // 查缓存，并把有缓存的input标识出来
            if(ret_type==PyUdfType::STRING){
              for(int j=0; j < cell->get_input_store().get_saved_size(); ++j){
                string value;
                const char* original_c_str=input_list_for_cells[count][j].c_str();
                size_t length = std::strlen(original_c_str) + 1;
                char* c_str = new char[length];
                std::strcpy(c_str, original_c_str);
                if(OB_FAIL(udf_cache.get_string(udf_name, c_str, value))){
                  if(OB_HASH_NOT_EXIST == ret){
                    // 如果没有udf级别的缓存结果，并且有找到已缓存中间结果的可能，就尝试查找是否存在中间结果
                    if(!info->is_new_mid_cache&&can_be_used_redundent_cache_map_is_found&&!info->udf_meta_.ismerged_){
                      float* mid_res_value=nullptr;
                      if(OB_FAIL(udf_cache.get_mid_result(can_be_used_model_path, c_str, mid_res_value))){
                        cells_cached_mid_res_bit_vector[count][j]=false;
                      }else{
                        cells_can_use_cache[count]++;
                        cells_cached_mid_res_bit_vector[count][j]=true;
                        count_cached_mid_res++;
                        cells_cached_res_for_mid_result[count][j]=mid_res_value;
                      }
                    }
                    cells_cached_res_bit_vector[count][j]=false;
                    ret=OB_SUCCESS;
                  }
                }else{
                  cells_can_use_cache[count]++;
                  cells_cached_res_bit_vector[count][j]=true;
                  cells_cached_res_for_str[count][j]=value;
                }
              }
            }else if(ret_type==PyUdfType::INTEGER){
              for(int j=0; j < cell->get_input_store().get_saved_size(); ++j){
                int value;
                const char* original_c_str=input_list_for_cells[count][j].c_str();
                size_t length = std::strlen(original_c_str) + 1;
                char* c_str = new char[length];
                std::strcpy(c_str, original_c_str);
                if(OB_FAIL(udf_cache.get_int(udf_name, c_str, value))){
                  if(OB_HASH_NOT_EXIST == ret){
                    // 如果没有udf级别的缓存结果，并且有找到已缓存中间结果的可能，就尝试查找是否存在中间结果
                    if(!info->is_new_mid_cache&&can_be_used_redundent_cache_map_is_found&&!info->udf_meta_.ismerged_){
                      float* mid_res_value=nullptr;
                      if(OB_FAIL(udf_cache.get_mid_result(can_be_used_model_path, c_str, mid_res_value))){
                        cells_cached_mid_res_bit_vector[count][j]=false;
                      }else{
                        cells_can_use_cache[count]++;
                        cells_cached_mid_res_bit_vector[count][j]=true;
                        count_cached_mid_res++;
                        cells_cached_res_for_mid_result[count][j]=mid_res_value;
                      }
                    }
                    cells_cached_res_bit_vector[count][j]=false;
                    ret=OB_SUCCESS;
                  }
                }else{
                  cells_can_use_cache[count]++;
                  cells_cached_res_bit_vector[count][j]=true;
                  cells_cached_res_for_int[count][j]=value;
                }
              }
            }else if(ret_type==PyUdfType::REAL){
              for(int j=0; j < cell->get_input_store().get_saved_size(); ++j){
                double value;
                const char* original_c_str=input_list_for_cells[count][j].c_str();
                size_t length = std::strlen(original_c_str) + 1;
                char* c_str = new char[length];
                std::strcpy(c_str, original_c_str);
                if(OB_FAIL(udf_cache.get_double(udf_name, c_str, value))){
                  if(OB_HASH_NOT_EXIST == ret){
                    // 如果没有udf级别的缓存结果，并且有找到已缓存中间结果的可能，就尝试查找是否存在中间结果
                    if(!info->is_new_mid_cache&&can_be_used_redundent_cache_map_is_found&&!info->udf_meta_.ismerged_){
                      float* mid_res_value=nullptr;
                      if(OB_FAIL(udf_cache.get_mid_result(can_be_used_model_path, c_str, mid_res_value))){
                        cells_cached_mid_res_bit_vector[count][j]=false;
                      }else{
                        cells_can_use_cache[count]++;
                        cells_cached_mid_res_bit_vector[count][j]=true;
                        count_cached_mid_res++;
                        cells_cached_res_for_mid_result[count][j]=mid_res_value;
                      }
                    }
                    cells_cached_res_bit_vector[count][j]=false;
                    ret=OB_SUCCESS;
                  }
                }else{
                  cells_can_use_cache[count]++;
                  cells_cached_res_bit_vector[count][j]=true;
                  cells_cached_res_for_double[count][j]=value;
                }
              }
            }
          }else if(info->udf_meta_.ismerged_){
            // 如果是经查询内冗余消除后的udf，就不对融合后的udf进行缓存，而是缓存融合前的每个udf
            cells_can_use_cache[count]=0;
            if(ret_type==PyUdfType::STRING){
              for(auto name: info->udf_meta_.merged_udf_names_){
                if(!udf_cache.find_cache_for_cell(name)){
                  udf_cache.create_cache_for_cell_for_str(name);
                  info->is_new_full_cache=true;
                }
              }
            }else if(ret_type==PyUdfType::INTEGER){
              for(auto name: info->udf_meta_.merged_udf_names_){
                if(!udf_cache.find_cache_for_cell(name)){
                  udf_cache.create_cache_for_cell_for_int(name);
                  info->is_new_full_cache=true;
                }   
              }
            }else if(ret_type==PyUdfType::REAL){
              for(auto name: info->udf_meta_.merged_udf_names_){
                if(!udf_cache.find_cache_for_cell(name)){
                  udf_cache.create_cache_for_cell_for_double(name);
                  info->is_new_full_cache=true;
                }
              }
            }
          }else{
            // 没初始化缓存，就初始化
            cells_can_use_cache[count]=0;
            if(ret_type==PyUdfType::STRING){
              if(!udf_cache.find_cache_for_cell(udf_name)){
                udf_cache.create_cache_for_cell_for_str(udf_name);
                info->is_new_full_cache=true;
              }
            }else if(ret_type==PyUdfType::INTEGER){
              if(!udf_cache.find_cache_for_cell(udf_name)){
                udf_cache.create_cache_for_cell_for_int(udf_name);
                info->is_new_full_cache=true;
              }
            }else if(ret_type==PyUdfType::REAL){
              if(!udf_cache.find_cache_for_cell(udf_name)){
                udf_cache.create_cache_for_cell_for_double(udf_name);
                info->is_new_full_cache=true;
              }
            }
            // 如果没有udf级别的缓存结果，并且有找到已缓存中间结果的可能，就尝试查找是否存在中间结果
            if(!info->is_new_mid_cache&&can_be_used_redundent_cache_map_is_found){
              for(int j=0; j < cell->get_input_store().get_saved_size(); ++j){
                float* mid_res_value=nullptr;
                const char* original_c_str=input_list_for_cells[count][j].c_str();
                size_t length = std::strlen(original_c_str) + 1;
                char* c_str = new char[length];
                std::strcpy(c_str, original_c_str);
                if(OB_FAIL(udf_cache.get_mid_result(can_be_used_model_path, c_str, mid_res_value))){
                  cells_cached_mid_res_bit_vector[count][j]=false;
                }else{
                  cells_can_use_cache[count]++;
                  cells_cached_mid_res_bit_vector[count][j]=true;
                  count_cached_mid_res++;
                  cells_cached_res_for_mid_result[count][j]=mid_res_value;
                }
              }
            }
          }
          count++;
        }
  return ret;
}

int ObPUStoreController::process()
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    /* do nothing */
  } else {
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      // todo 要是走缓存的话，应该修改每个cell的input_store_，这就可能导致
      // cell->get_store_size() != stored_input_cnt_不成立
      if (cell->get_store_size() != stored_input_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unsaved input rows.", K(ret));
      } else if (with_batch_control_ && OB_FAIL(cell->do_process())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process udf failed.", K(ret));
      } else if (!with_batch_control_ && OB_FAIL(cell->do_process_all())) {  // without predict size control
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell process all udf failed.", K(ret));
      } else if (cell->get_result_size() != stored_input_cnt_){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unprocessed input rows.", K(ret));
      } else {
        cell->reset_input_store();
      }
    }
    if (OB_SUCC(ret)) {
      stored_output_cnt_ = stored_input_cnt_;
      stored_input_cnt_ = 0;
      output_idx_ = 0;
    }
  }
  return ret;
}

int ObPUStoreController::restore(ObEvalCtx &eval_ctx, ObBatchRows &brs, int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (stored_output_cnt_ <= 0 || !can_output()) { // Not enough output data
    /* do nothing */
  } else {
    // output_size > 0
    //int64_t output_size = batch_size_ < (stored_output_cnt_ - output_idx_) 
    //                     ? batch_size_ 
    //                     : (stored_output_cnt_ - output_idx_);
    int64_t output_size = std::min(max_row_cnt, stored_output_cnt_ - output_idx_); 
    ObPythonUDFCell* header = cells_list_.get_header();
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
      if (OB_FAIL(cell->do_restore(eval_ctx, output_idx_, output_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Do Python UDF Cell restore failed.", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(other_store_.load_vector(eval_ctx, output_size))) {
      //if (OB_FAIL(other_store_.load_batch(eval_ctx, output_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Load other input cols failed.", K(ret));
      } else {
        output_idx_ += output_size;
        // reset batchrows
        brs.size_ = output_size;
        brs.reset_skip(output_size);
        brs.end_ = false;
      }
    }
    if (OB_SUCC(ret) && end_output()) {
      other_store_.reuse();
    }
  }
  return ret;
}

int ObPUStoreController::restore_with_cache(ObEvalCtx &eval_ctx, ObBatchRows &brs, int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (stored_output_cnt_ <= 0 || !can_output()) { // Not enough output data
    /* do nothing */
  } else {
    int64_t output_size = std::min(max_row_cnt, stored_output_cnt_ - output_idx_); 
    ObPythonUDFCell* header = cells_list_.get_header();
    int count=0;
    for (ObPythonUDFCell* cell = cells_list_.get_first(); 
        cell != header && OB_SUCC(ret); 
        cell = cell->get_next()) {
          if(with_full_funcache_||with_fine_funcache_){
            if (OB_FAIL(cell->do_restore_with_cache(cells_can_use_cache[count]>0, eval_ctx, output_idx_, output_size, cells_cached_res_for_double[count],
            cells_cached_res_for_int[count], cells_cached_res_for_str[count], input_list_for_cells[count], cells_cached_res_bit_vector[count], cells_cached_mid_res_bit_vector[count]))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Do Python UDF Cell restore failed.", K(ret));
            }
          }else{
            if (OB_FAIL(cell->do_restore(eval_ctx, output_idx_, output_size))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Do Python UDF Cell restore failed.", K(ret));
            }
          }
          count++;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(other_store_.load_vector(eval_ctx, output_size))) {
      //if (OB_FAIL(other_store_.load_batch(eval_ctx, output_size))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Load other input cols failed.", K(ret));
      } else {
        output_idx_ += output_size;
        // reset batchrows
        brs.size_ = output_size;
        brs.reset_skip(output_size);
        brs.end_ = false;
      }
    }
    if (OB_SUCC(ret) && end_output()) {
      other_store_.reuse();
    }
  }
  return ret;
}

int ObPUStoreController::resize(int64_t size)
{
  int ret = OB_SUCCESS;
  ObPythonUDFCell* header = cells_list_.get_header();
  for (ObPythonUDFCell* cell = cells_list_.get_first(); 
      cell != header && OB_SUCC(ret); 
      cell = cell->get_next()) {
    if (OB_FAIL(cell->reset(size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Do Python UDF Cell resize failed.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = other_store_.reset(size);
  }
  stored_input_cnt_ = 0;
  stored_output_cnt_ = 0;
  output_idx_ = 0;
  return ret;
}

} // end namespace sql
} // end namespace oceanbase