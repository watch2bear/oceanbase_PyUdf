#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_pyudf_dtpo.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"

#include "sql/rewrite/onnx_optimizer/onnxoptimizer/optimize_c_api/optimize_c_api.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObTransformPyUdfDTPO::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(onnx_decision_tree_prune(stmt))) {
    LOG_WARN("failed to do decision tree pruning", K(ret));
  } else {}
  return ret;
}

int ObTransformPyUdfDTPO::transform_one_stmt_with_outline(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                          ObDMLStmt *&stmt,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
    OPT_TRACE("not select stmt, can not transform");
  } else if (OB_FAIL(onnx_decision_tree_prune(stmt))) {
    LOG_WARN("failed to do decision tree pruning", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to do python udf DTPO with outline", K(ctx_->src_qb_name_));
  }
  return ret;
}

// 不考虑python udf内出现python udf，这种情况会在resolve阶段就弹出
int ObTransformPyUdfDTPO::onnx_decision_tree_prune(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
  ObIArray<ObRawExpr *> &python_udf_filter_exprs = select_stmt->get_python_udf_filter_exprs();
  ObRawExpr *filter_expr = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < python_udf_filter_exprs.count(); ++i) {
    filter_expr = python_udf_filter_exprs.at(i);
    ret = do_recursive_onnx_decision_tree_prune(select_stmt, nullptr, filter_expr);
  }
  return ret;
}

int ObTransformPyUdfDTPO::do_recursive_onnx_decision_tree_prune(ObSelectStmt *select_stmt,
                                                                ObRawExpr *parent_expr,
                                                                ObRawExpr *cmp_expr) {
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = nullptr;
  ObRawExpr *right_expr = nullptr;
  ObPythonUdfRawExpr *python_udf_expr = nullptr;
  ObRawExpr *value_expr = nullptr;
  ObConstRawExpr *const_expr = nullptr;
  if (IS_BASIC_CMP_OP(cmp_expr->get_expr_type()) && 
      cmp_expr->get_param_count() == 2 &&
      (left_expr = cmp_expr->get_param_expr(0)) != nullptr && 
      (right_expr = cmp_expr->get_param_expr(1)) != nullptr &&
      ((left_expr->get_expr_type() == T_FUN_PYTHON_UDF && right_expr->is_const_expr()) || 
       (left_expr->is_const_expr() && right_expr->get_expr_type() == T_FUN_PYTHON_UDF))) {
    // get python udf expr and const expr
    if (left_expr->get_expr_type() == T_FUN_PYTHON_UDF) {
      python_udf_expr = static_cast<ObPythonUdfRawExpr *>(left_expr);
      value_expr = right_expr;
    } else {
      python_udf_expr = static_cast<ObPythonUdfRawExpr *>(right_expr);
      value_expr = left_expr;
    }
    // check udf meta
    ObPythonUDFMeta &udf_meta = python_udf_expr->get_udf_meta();
    if (udf_meta.model_type_ == ObPythonUdfEnumType::PyUdfUsingType::MODEL_SPECIFIC &&
      udf_meta.udf_model_meta_.count() == 1 &&
      udf_meta.udf_model_meta_.at(0).framework_ == ObPythonUdfEnumType::ModelFrameworkType::ONNX &&
      udf_meta.udf_model_meta_.at(0).model_type_ == ObPythonUdfEnumType::ModelType::RANDOM_FOREST &&
      !udf_meta.is_retree_opt_) {
      
      // prepare onnx decision tree pruning
      uint8_t comparison_operator = 0;
      if (OB_SUCC(ret)) {
        switch(cmp_expr->get_expr_type()) {
        case T_OP_EQ:
        case T_OP_NSEQ:
          comparison_operator = 0;
          break;
        case T_OP_LT:
          comparison_operator = 1;
          break;
        case T_OP_LE:
          comparison_operator = 2;
          break;
        case T_OP_GT:
          comparison_operator = 3;
          break;
        case T_OP_GE:
          comparison_operator = 4;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unsupported comparison operator", K(ret));
        }
      }

      char buffer[128];
      float threshold = 0;
      if (OB_SUCC(ret)) {
        ObItemType expr_type = value_expr->get_expr_type();
        // ObObjType data_type = value_expr->get_data_type();
        // find in plain number & inside 1 CAST
        if (expr_type == T_FUN_SYS_CAST) {
          value_expr = value_expr->get_param_expr(0);
          expr_type = value_expr->get_expr_type();
        }
        const_expr = dynamic_cast<ObConstRawExpr *>(value_expr);
        if (const_expr == nullptr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can not find const expr", K(ret));
        } else {
          ObObj &val = const_expr->get_value();
          int64_t pos = 0;
          switch (expr_type) {
          case T_INT:
            threshold = float(val.get_int());
            break;
          case T_NUMBER:
            if (OB_FAIL(wide::to_string(val.get_decimal_int(), val.get_int_bytes(),
                                        val.get_scale(), buffer, sizeof(buffer), pos))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("transform from decimal to string failed", K(ret));
            } else {
              threshold = std::atof(buffer);
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unsupported value expr item type", K(ret));
          }
        }
      }

      // do onnx decision tree pruning
      if (OB_SUCC(ret)) {
        std::string input_model_path = std::string(udf_meta.udf_model_meta_.at(0).model_path_.ptr(), udf_meta.udf_model_meta_.at(0).model_path_.length());
        std::string output_path = optimize_on_decision_tree_predicate(input_model_path, comparison_operator, threshold);
        if (OB_FAIL(ob_write_string(*ctx_->allocator_, ObString(output_path.c_str()), udf_meta.udf_model_meta_.at(0).model_path_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to write optimized model path", K(output_path.c_str()), K(ret));
        } else {
          udf_meta.is_retree_opt_ = true;
          // do expr replacement
          if (parent_expr == nullptr) {
            // 修改stmt python udf filter exprs
            ObIArray<ObRawExpr *> &filters = select_stmt->get_python_udf_filter_exprs();
            for (int i = 0; i < filters.count(); ++i) {
              if (filters.at(i) == cmp_expr) {
                filters.remove(i);
                ObConstRawExpr *const_expr = NULL;
                ObRawExpr *greater_expr = NULL;
                ObPythonUDFMeta &udf_meta=python_udf_expr->get_udf_meta();
                python_udf_expr->get_udf_meta().ret_ = ObPythonUdfEnumType::PyUdfRetType::REAL;
                python_udf_expr->set_data_type(ObDoubleType);
                if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*ctx_->expr_factory_,
                                                               ObDoubleType,
                                                               0.5,
                                                               const_expr))) {
                        LOG_WARN("failed to build const bool expr", K(ret));
                }else if (OB_FAIL(ObRawExprUtils::create_greater_expr(*(ctx_->expr_factory_),
                                                                      ctx_->session_info_,
                                                                      const_expr,
                                                                      python_udf_expr,
                                                                      greater_expr))) {
                    LOG_WARN("Creation of equal expr for expr_opted fails", K(ret));
                }
                filters.push_back(greater_expr);
                break;
              }
            }
          } else {
            // 修改parent expr的child expr
            for (int i = 0; i < parent_expr->get_param_count(); ++i) {
              if (parent_expr->get_param_expr(i) == cmp_expr) {
                //python_udf_expr->get_udf_meta().ret_ = ObPythonUdfEnumType::PyUdfRetType::INTEGER;
                //python_udf_expr->set_data_type(ObTinyIntType);
                python_udf_expr->get_udf_meta().ret_ = ObPythonUdfEnumType::PyUdfRetType::REAL;
                python_udf_expr->set_data_type(ObDoubleType);
                ObConstRawExpr *const_expr = NULL;
                ObRawExpr *greater_expr = NULL;
                if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*ctx_->expr_factory_,
                                                               ObDoubleType,
                                                               0.5,
                                                               const_expr))) {
                        LOG_WARN("failed to build const bool expr", K(ret));
                }else if (OB_FAIL(ObRawExprUtils::create_greater_expr(*(ctx_->expr_factory_),
                                                                      ctx_->session_info_,
                                                                      const_expr,
                                                                      python_udf_expr,
                                                                      greater_expr))) {
                    LOG_WARN("Creation of equal expr for expr_opted fails", K(ret));
                }
                parent_expr->get_param_expr(i) = greater_expr;
                break;
              }
            }
          }
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cmp_expr->get_param_count(); ++i) {
      if (OB_FAIL(do_recursive_onnx_decision_tree_prune(select_stmt, cmp_expr, cmp_expr->get_param_expr(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected error in onnx decision tree pruning.", K(ret));
      }
    }
  }
  //return ret;
  return OB_SUCCESS;
}

int ObTransformPyUdfDTPO::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         const ObDMLStmt &stmt,
                                         bool &need_trans)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("Check need transform of ObTransformPyUdfDTPO.", K(ret));
  need_trans = false;
  if (stmt.is_select_stmt()) {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(stmt);
    const ObIArray<ObRawExpr *> &python_udf_filters = select_stmt.get_python_udf_filter_exprs();
    if (python_udf_filters.count() != 0) {
      const ObIArray<ObPythonUdfRawExpr *> &python_udf_exprs = select_stmt.get_python_udf_exprs();
      for (int64_t i = 0; i < python_udf_exprs.count(); ++i) {
        // check udf meta
        ObPythonUDFMeta &udf_meta = python_udf_exprs.at(i)->get_udf_meta();
        if (udf_meta.model_type_ == ObPythonUdfEnumType::PyUdfUsingType::MODEL_SPECIFIC &&
        udf_meta.udf_model_meta_.count() == 1 &&
        udf_meta.udf_model_meta_.at(0).framework_ == ObPythonUdfEnumType::ModelFrameworkType::ONNX &&
        udf_meta.udf_model_meta_.at(0).model_type_ == ObPythonUdfEnumType::ModelType::RANDOM_FOREST &&
        !udf_meta.is_retree_opt_) {
          need_trans = true;
          break;
        }
      }
    } else {}
  }
  if (need_trans) {
    LOG_DEBUG("this query need transform of ObTransformPyUdfDTPO");
  }
  return ret;
}

int ObTransformPyUdfDTPO::construct_transform_hint(ObDMLStmt &stmt, void *trans_params) 
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> *transed_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(transed_stmts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->add_src_hash_val(ctx_->src_qb_name_))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else {
    ObTransHint *hint = NULL;
    ObString qb_name;
    ObSelectStmt *cur_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < transed_stmts->count(); i++) {
      if (OB_ISNULL(cur_stmt = transed_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transed_stmt is null", K(ret), K(i));
      } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(cur_stmt->get_stmt_hint())))) {
        LOG_WARN("failed to add used hint", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, get_hint_type(), hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (OB_FAIL(cur_stmt->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(cur_stmt->adjust_qb_name(ctx_->allocator_,
                                                  qb_name,
                                                  ctx_->src_hash_val_))) {
        LOG_WARN("adjust stmt id failed", K(ret));
      } else {
        hint->set_qb_name(qb_name);
      }
    }
  }
  return ret;
}
}
}