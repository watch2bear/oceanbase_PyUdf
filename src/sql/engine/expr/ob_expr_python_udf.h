#ifndef _OB_EXPR_PYTHON_UDF_
#define _OB_EXPR_PYTHON_UDF_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/python_udf_engine/python_udf_engine.h"

namespace  oceanbase {
namespace  sql {
class  ObExprPythonUdf : public  ObExprOperator {
public:
  explicit  ObExprPythonUdf(common::ObIAllocator &alloc);
  virtual  ~ObExprPythonUdf();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const;
  
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const  override;

public:
  int set_udf_meta(const share::schema::ObPythonUDFMeta &udf);

  static int deep_copy_udf_meta(share::schema::ObPythonUDFMeta &dst,
                                common::ObIAllocator &alloc,
                                const share::schema::ObPythonUDFMeta &src);
                                
  int init_udf(const common::ObIArray<ObRawExpr*> &param_exprs);

  static int import_udf(const share::schema::ObPythonUDFMeta &udf_meta);

  static int eval_python_udf(const ObExpr &expr,
                             ObEvalCtx &ctx,
                             ObDatum &res);

  static int eval_python_udf_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const int64_t batch_size);

  static int eval_python_udf_batch_buffer(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const int64_t batch_size);

  static int get_python_udf(pythonUdf* &pyudf, const ObExpr& expr);

  static int eval_test_udf(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

  static int eval_test_udf_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                 const ObBitVector &skip, const int64_t batch_size);

  static void message_error_dialog_show(char* buf);

  static void process_python_exception();

protected:
  common::ObIAllocator &allocator_;
  share::schema::ObPythonUDFMeta udf_meta_;
};

struct ObPythonUdfInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPythonUdfInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type), allocator_(alloc), udf_meta_() {}
  ~ObPythonUdfInfo() {}

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  int from_raw_expr(const ObPythonUdfRawExpr &expr);

  common::ObIAllocator &allocator_;
  share::schema::ObPythonUDFMeta udf_meta_;
};
} /* namespace sql */
} /* namespace oceanbase */

#endif