// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "json.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <list>
#include <locale>
 
using json = nlohmann::json;

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"

#include "../../../../src/include/access/htup_details.h"
#include "../../../../src/include/access/reloptions.h"
#include "../../../../src/include/access/sysattr.h"
#include "../../../../src/include/access/table.h"
#include "../../../../src/include/access/nbtree.h"
#include "../../../../src/include/catalog/pg_authid.h"
#include "../../../../src/include/catalog/pg_foreign_table.h"
#include "../../../../src/include/catalog/pg_type.h"
#include "../../../../src/include/commands/copy.h"
#include "../../../../src/include/commands/defrem.h"
#include "../../../../src/include/commands/explain.h"
#include "../../../../src/include/commands/vacuum.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"
#include "../../../../src/include/miscadmin.h"
#include "../../../../src/include/nodes/makefuncs.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/restrictinfo.h"
#include "../../../../src/include/parser/parse_coerce.h"
#include "../../../../src/include/parser/parse_func.h"
#include "../../../../src/include/parser/parse_oper.h"
#include "../../../../src/include/parser/parse_type.h"
#include "../../../../src/include/utils/acl.h"
#include "../../../../src/include/utils/memutils.h"
#include "../../../../src/include/utils/rel.h"
#include "../../../../src/include/utils/sampling.h"
#include "../../../../src/include/utils/builtins.h"
#include "../../../../src/include/utils/typcache.h"
#include "../../../../src/include/utils/lsyscache.h"
#include "../../../../src/include/utils/jsonb.h"
}
// clang-format on

#define BLKSIZE (1024 * 1024)

typedef unsigned char uint8;
typedef unsigned short uint16;
typedef unsigned int uint32;

typedef double dCost;

typedef enum DataType
{
  Type_Str = 1,
  Type_int,
  Type_float,
  Type_Invalid
} DataType;

typedef struct db721Filter
{
  AttrNumber  attnum;
  bool        is_key;
  Const       *value;
  int         strategy;
  size_t      strVallen;
} db721Filter;

typedef struct BlockInfo
{
  union {
    struct {
      uint32  nMin; //
      uint32  nMax; //
    } int_val;
    struct {
      float   nMin; //
      float   nMax; //
    } float_val;
  } min_max;
  char strMin[36]; //
  char strMax[36]; //
} BlockInfo;

typedef struct BlockBufferInfo
{
  char        *data; //
  char        *attname; //
  uint32      nStartOffset; //
  uint32      ncursor; //
  DataType    eType; //
  uint32      ntypeSize; //
  uint32      nBlock; //
  uint32      nBlockIdx; //
  uint32      nBufferSize;
  bool        bLoaded; //
}BlockBufferInfo;

typedef struct ColumnBufferMgr
{
  uint32 nColumn;
  uint32 nBlock;
  uint32 nMaxValueInBlock;
  BlockBufferInfo **blockBufferInfoArray;
  bool *scanInfoArr;
} ColumnBufferMgr;

typedef struct db721PlanState
{
  char    *filename;
  char    *tblname;
  json    metadata;
  List    *columnList;
  List    *newscan_clause;
  List    *relRowGroup;
  List    *relResultGroup;
  List    *filters;

  uint32  nRawData;
  uint32  nBlock;
  double  nTuple;
  Oid     foreigntableid;
} db721PlanState;

typedef struct db721ScanState
{
  char    *filename;
  char    *tblname;
  json    metadata;

  FILE        *pFile;
  TupleDesc   tupleDesc;

  List *rowgroup;
  List *columnList;
  List *whereClauseList;
  List *filters;

  ColumnBufferMgr *coulmnBufferMgr;

  bool *checkResult;
} db721ScanState;

Datum
getDatumForType(DataType eType, BlockInfo &stBlockInfo, bool bmin, bool bStrlen)
{
  Datum result = 0;

  if (eType == Type_Str)
  {
    if (bStrlen)
    {
      if (bmin)
        result = Int32GetDatum(stBlockInfo.min_max.int_val.nMin);        
      else
        result = Int32GetDatum(stBlockInfo.min_max.int_val.nMax);      
    } else 
    {
      if (bmin)
        result = CStringGetTextDatum(stBlockInfo.strMin);
      else
        result = CStringGetTextDatum(stBlockInfo.strMax);
    }
  } else if (eType == Type_int)
  {
    if (bmin)
      result = Int32GetDatum(stBlockInfo.min_max.int_val.nMin);        
    else
      result = Int32GetDatum(stBlockInfo.min_max.int_val.nMax);          
  } else if (eType == Type_float)
  {
    if (bmin)
      result = Float4GetDatum(stBlockInfo.min_max.float_val.nMin);        
    else
      result = Float4GetDatum(stBlockInfo.min_max.float_val.nMax);          
  }

  return result;
}

// void
// print_log(Datum min_max, DataType eType, char *attname, Datum con, int strategy)
// {
//   if (eType == Type_Str)
//   {
//     elog(LOG, "st: %d, attrname: %s, const: %s, key: %s", strategy, attname, TextDatumGetCString(con), TextDatumGetCString(min_max));
//   }
//   else if (eType == Type_int)
//   {
//     elog(LOG, "st: %d, attrname: %s, const: %d, key: %d", strategy, attname, DatumGetInt32(con), DatumGetInt32(min_max));
//   }
//   else
//   {
//     elog(LOG, "st: %d, attrname: %s, const: %f, key: %f", strategy, attname, DatumGetFloat8(con), DatumGetFloat4(min_max));
//   }
// }

DataType
getDataType(const char *type)
{
  DataType eResult = Type_Invalid;

  if (strcmp(type, "str") == 0)
  {
    eResult = Type_Str;
  } else if (strcmp(type,"float") == 0)
  {
    eResult = Type_float;
  } else if (strcmp(type,"int") == 0)
  {
    eResult = Type_int;
  }

  return eResult;
}

template<typename CharT>
int 
locale_compare(const std::locale& l, const CharT* p1, const CharT* p2)
{
  auto& f = std::use_facet<std::collate<CharT>>(l);

  std::basic_string<CharT> s1(p1), s2(p2);
  return f.compare(&s1[0], &s1[0] + s1.size(),
                   &s2[0], &s2[0] + s2.size() );
}

static void 
db721GetOptions(Oid foreigntableid,
                char **filename, char **tblname)
{
  ForeignTable        *table;
  ForeignServer       *server;

  List      *options;
  ListCell  *lc;

  table = GetForeignTable(foreigntableid);
  server = GetForeignServer(table->serverid);

  options = NIL;
  options = list_concat(options, table->options);
  options = list_concat(options, server->options);

  *filename = NULL;
  *tblname = NULL;

  foreach(lc, options)
  {
    DefElem *def = (DefElem *) lfirst(lc);

    if (strcmp(def->defname, "filename") == 0)
    {
      *filename = defGetString(def);
      options = foreach_delete_current(options, lc);
      // elog(LOG, "  cur filename: %s", *filename);
      // break;
    }
    else if (strcmp(def->defname, "tablename") == 0)
    {
      *tblname = defGetString(def);
      options = foreach_delete_current(options, lc);
      // elog(LOG, "  cur tblname: %s", *tblname);
      // break;
    }
  }
}

static double
getEstimatedTuple(RelOptInfo *baserel, db721PlanState *fdw_private)
{
  double nTuple = 0.0;
  double nBlock = fdw_private->nBlock;

  if (baserel->pages > 0)
  {
    double tupleDensity = baserel->tuples / (double) baserel->pages;

    nTuple = clamp_row_est(tupleDensity * (double) nBlock);
  }
  else
  {
    nTuple = fdw_private->nTuple;
  }

  return nTuple;
}

static int
db721GetMeta(char *filepath, 
             json *metadata)
{
  FILE *pFile = NULL;
  // char str[4];
  char *buff = NULL;
  int nMetaOffset;
  int nRawData = 0;
  uint32 nMeta;
  uint32 nIdx;

  pFile = fopen(filepath, "r");

  fseek(pFile, -4, SEEK_END);
  // fgets((char *) &nMeta, 4, pFile);
  buff = (char *)&nMeta;
  for(nIdx = 0; nIdx < 4; nIdx++)
  {
    buff[nIdx] = fgetc(pFile);
  }

  // elog(LOG, "    parsing size: %u", nMeta);

  buff = (char *) palloc0(nMeta * sizeof(char) + VARHDRSZ);

  nMetaOffset = (nMeta + 4) * -1;
  fseek(pFile, nMetaOffset, SEEK_END);
  nRawData = ftell(pFile);

  // fgets(buff, nMeta + 1, pFile);
  for (nIdx = 0; nIdx < nMeta; nIdx++)
  {
    buff[nIdx] = fgetc(pFile);
  }
  buff[nMeta] = '\0';

  *metadata = json::parse(buff);

  fclose(pFile);

  return nRawData;
}

static int
get_strategy(Oid type, Oid opno, Oid am)
{
  Oid opclass;
  Oid opfamily;

  opclass = GetDefaultOpClass(type, am);

  if (!OidIsValid(opclass))
    return 0;

  opfamily = get_opclass_family(opclass);

  return get_op_opfamily_strategy(opno, opfamily);
}

static bool
deleteElemClause(List *scan_clauses,
                 db721PlanState *fdw_private,
                 RelOptInfo *baserel)
{
  ListCell *clauseCell = NULL;
  bool bFound = false;

  foreach (clauseCell, scan_clauses)
  {
    Expr       *clause = (Expr *) lfirst(clauseCell);

    if (IsA(clause, RestrictInfo))
    {
      clause = ((RestrictInfo *) clause)->clause;
    }

    if (IsA(clause, OpExpr))
    {
      OpExpr *expr = (OpExpr *) clause;
      Expr   *leftExpr, *rightExpr;

      if (list_length(expr->args) != 2)
      {
        continue;
      }

      leftExpr = (Expr *) list_nth(expr->args, 0);
      rightExpr = (Expr *) list_nth(expr->args, 1);

      if (IsA(rightExpr, Const)) // Expr op Const 
      {
        if (IsA(leftExpr, RelabelType))
        {
          // leftExpr = ((RelabelType *)leftExpr)->arg;
        }
        else if (!IsA(leftExpr, Var)) // invalid case
        {
          continue;
        }
      }
      else if (IsA(leftExpr, Const)) // Const op Expr
      {
        if (IsA(rightExpr, RelabelType))
        {
          // rightExpr = ((RelabelType *)rightExpr)->arg;
        }
        else if (!IsA(rightExpr, Var)) // invalid case
        {
          continue;
        }
      }
      else // expr op expr case
      {
          continue;
      }
    }
    else
    {
      continue;
    }

    baserel->baserestrictinfo = list_delete_cell(scan_clauses, clauseCell);

    bFound = true;
    return bFound;
  }

  return bFound;
}

static bool
extract_db721_filters(List *scan_clauses,
                      db721PlanState *fdw_private,
                      RelOptInfo *rel)
{
  ListCell *clauseCell = NULL;
  List *filters = NIL;
  bool bFound = false;

  // scan_clauses = extract_actual_clauses(scan_clauses, false);

  foreach (clauseCell, scan_clauses)
  {
    Expr       *clause = (Expr *) lfirst(clauseCell);
    int         strategy;
    Const      *con;
    Var        *var;
    Oid         opno;
    bool        is_key = false;
    size_t      strLen = 0;

    // elog(LOG, " check clause");

    if (IsA(clause, RestrictInfo))
    {
      // elog(LOG, " check RestrictInfo");
      clause = ((RestrictInfo *) clause)->clause;
    }

    if (clause == NULL)
    {
      elog(LOG, "      check Expr is NULL");      
    }

    if (IsA(clause, OpExpr))
    {
      OpExpr *expr = (OpExpr *) clause;
      Expr   *leftExpr, *rightExpr;

      if (list_length(expr->args) != 2)
      {
        // TODO: check or and options
        elog(LOG, "      check length");
        continue;
      }

      leftExpr = (Expr *) list_nth(expr->args, 0);
      rightExpr = (Expr *) list_nth(expr->args, 1);

      if (IsA(rightExpr, Const)) // Expr op Const 
      {
        if (IsA(leftExpr, RelabelType)) // assume string comapre
        { 
          leftExpr = ((RelabelType *)leftExpr)->arg;

          strLen = strlen(TextDatumGetCString(((Const *) rightExpr)->constvalue));
        }
        else if (!IsA(leftExpr, Var)) // invalid case
        {
          elog(LOG, "      check invalid left");
          continue;
        }

        var = (Var *) leftExpr;
        con = (Const *) rightExpr;
        opno = expr->opno;
      }
      else if (IsA(leftExpr, Const)) // Const op Expr
      {
        if (IsA(rightExpr, RelabelType)) // assume string compare
        {
          rightExpr = ((RelabelType *)rightExpr)->arg;

          strLen = strlen(TextDatumGetCString(((Const *) leftExpr)->constvalue));
        }
        else if (!IsA(rightExpr, Var)) // invalid case
        {
          elog(LOG, "      check invalid right");
          continue;
        }

        var = (Var *) rightExpr;
        con = (Const *) leftExpr;
        opno = get_commutator(expr->opno);
      }
      else // expr op expr case
      {
        elog(LOG, "    expr expr case");
        continue;
      }

      strategy = get_strategy(var->vartype, opno, BTREE_AM_OID);

      // if ((strategy = get_strategy(var->vartype, opno, BTREE_AM_OID)) == 0)
      // {
      //   elog(LOG, " check err %d", strategy);        
      //   if ((strategy = get_strategy(var->vartype, opno, GIN_AM_OID)) == 0
      //       || strategy != JsonbExistsStrategyNumber)
      //   {
      //     elog(LOG, " all strategy not matched %d", strategy);
      //     continue;
      //   }
      //   is_key = true;
      // }
    }
    else
    {
      elog(LOG, "other expression");
      continue;
    }

    // elog(LOG, " check strategy %d", strategy);

    db721Filter *filter = (db721Filter *) palloc0(sizeof(db721Filter));
    filter->attnum = var->varattno;
    filter->is_key = is_key;
    filter->value = con;
    filter->strategy = strategy;
    filter->strVallen = strLen;

    bFound = true;

    scan_clauses = 
    filters = lappend(filters, filter);
    // try {

    //   elog(LOG, " check strategy error check");
    //   filters.push_back(singlefilter);
    // } catch (std::exception &e) {
    //   elog(ERROR, "extracting row filters failed");
    // }
  }

  fdw_private->filters = filters;

  return bFound;
}

static List *
getColumList(RelOptInfo *baserel, Oid foreigntableid, db721PlanState *fdw_private)
{
  List *columnList = NIL;
  List *actualcolumnList = NIL;
  AttrNumber columnIdx;
  AttrNumber nColumn = baserel->max_attr;
  List *targetcolumnList = baserel->reltarget->exprs;

  ListCell *targetCell = NULL;
  List *restrictInfoList = baserel->baserestrictinfo;
  ListCell *restrictCell = NULL;
  const AttrNumber wholeRow = 0;
  Relation relation = table_open(foreigntableid, AccessShareLock);
  TupleDesc tupledesc = RelationGetDescr(relation);

  // add the columns used
  foreach(targetCell, targetcolumnList)
  {
    List *targetList = NIL;
    Node *targetExpr = (Node *) lfirst(targetCell);

    targetList = pull_var_clause(targetExpr,
                                 PVC_RECURSE_AGGREGATES |
                                 PVC_RECURSE_PLACEHOLDERS);
    actualcolumnList = list_union(actualcolumnList, targetList);
  }

  List *resultGroup = NIL;
  ListCell *lc;
  foreach(lc, actualcolumnList)
  {
    Var *var = (Var *) lfirst(lc);
    // elog(LOG, "check column info result %d", var->varattno);

    resultGroup = lappend_int(resultGroup, var->varattno);
  }
  fdw_private->relResultGroup = resultGroup;

  foreach(restrictCell, restrictInfoList)
  {
    RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictCell);
    Node *restrictClause = (Node *) restrictInfo->clause;
    List *clauseColumnList = NIL;

    clauseColumnList = pull_var_clause(restrictClause,
                                       PVC_RECURSE_AGGREGATES |
                                       PVC_RECURSE_PLACEHOLDERS);
    actualcolumnList = list_union(actualcolumnList, clauseColumnList);
  }

  // foreach(lc, actualcolumnList)
  // {
  //   Var *var = (Var *) lfirst(lc);
  //   elog(LOG, "check column info added %d", var->varattno);
  // }

  for (columnIdx = 1; columnIdx <= nColumn; columnIdx++)
  {
    ListCell *actualColumnCell = NULL;
    Var *column = NULL;
    Form_pg_attribute attributeForm = TupleDescAttr(tupledesc, columnIdx - 1);

    if (attributeForm->attisdropped)
    {
      continue;
    }

    foreach(actualColumnCell, actualcolumnList)
    {
      Var *actualColumn = (Var *) lfirst(actualColumnCell);
      if (actualColumn->varattno == columnIdx)
      {
        column = actualColumn;
        break;
      }
      else if (actualColumn->varattno == wholeRow)
      {
        Index tableId = actualColumn->varno;

        column = makeVar(tableId, columnIdx, attributeForm->atttypid,
                         attributeForm->atttypmod, attributeForm->attcollation,
                         0);
        break;
      }
    }

    if (column != NULL)
    {
      columnList = lappend(columnList, column);
    }
  }

  table_close(relation, AccessShareLock);

  return columnList;
}

static uint32
getRelationCount(Oid foreigntableid)
{
  Relation relation = table_open(foreigntableid, AccessShareLock);
  uint32 nRelationColumn = RelationGetNumberOfAttributes(relation);
  table_close(relation, AccessShareLock);
  return nRelationColumn;
}

List *
extract_db721_validblock(db721PlanState *fdw_private, List *filters)
{
  json *metadata = &fdw_private->metadata;
  List *rowgroups = NIL;
  Oid  foreigntableid = fdw_private->foreigntableid;
  Relation relation = table_open(foreigntableid, AccessShareLock);
  TupleDesc tupledesc = RelationGetDescr(relation);
  bool *scanBlockInfo = NULL;
  int *rowCnt = NULL;
  uint32 nBlock = 0;
  ListCell *filterCell = NULL;
  
  fdw_private->nTuple = 0.0;

  foreach (filterCell, filters)
  {
    db721Filter *filter = (db721Filter *) lfirst(filterCell);
    uint32 columnIdx = filter->attnum - 1;
    Form_pg_attribute attr = TupleDescAttr(tupledesc, columnIdx);
    char *attname = NameStr(attr->attname);
    json curColumn = (*metadata)["Columns"][attname];
    DataType eType = getDataType(curColumn["type"].get<std::string>().c_str());
    BlockInfo stBlockInfo;
    nBlock = curColumn["num_blocks"].get<int>();

    if (scanBlockInfo == NULL)
    {
      scanBlockInfo = (bool *) palloc0(nBlock * sizeof(bool));
      rowCnt = (int *) palloc0(nBlock * sizeof(int));
      for (uint32 nIdx = 0; nIdx < nBlock; nIdx++)
      {
        scanBlockInfo[nIdx] = true;
      }
    }
    
    for (uint32 nIdx = 0; nIdx < nBlock; nIdx++)
    {
      if (scanBlockInfo[nIdx] == false)
      {
        continue;
      }

      std::string IdxStr = std::to_string(nIdx);
      json blockStat = curColumn["block_stats"][IdxStr.c_str()];
      rowCnt[nIdx] = blockStat["num"].get<int>();

      // parsing statistics
      if (eType == Type_Str)
      {
        stBlockInfo.min_max.int_val.nMin = blockStat["min_len"].get<int>();
        stBlockInfo.min_max.int_val.nMax = blockStat["max_len"].get<int>();
        strcpy(stBlockInfo.strMin, blockStat["min"].get<std::string>().c_str());
        strcpy(stBlockInfo.strMax, blockStat["max"].get<std::string>().c_str());
      }
      else if (eType == Type_int)
      {
        stBlockInfo.min_max.int_val.nMin = blockStat["min"].get<int>();
        stBlockInfo.min_max.int_val.nMax = blockStat["max"].get<int>();
      }
      else if (eType == Type_float)
      {
        stBlockInfo.min_max.float_val.nMin = blockStat["min"].get<float>();
        stBlockInfo.min_max.float_val.nMax = blockStat["max"].get<float>();
      }

      // check matching with filter
      Datum val = filter->value->constvalue;
      bool bSatisfied = false;

      switch (filter->strategy)
      {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
        {
          // Datum   lower;

          // lower = getDatumForType(eType, stBlockInfo, true, false);
          // print_log(lower, eType, attname, val, filter->strategy);

          if (eType == Type_Str)
          {
            {
              int result = locale_compare(std::locale(), stBlockInfo.strMin, TextDatumGetCString(val));

              if (filter->strategy == BTLessStrategyNumber)
              {
                bSatisfied = result < 0;
              }
              else 
              {
                bSatisfied = result <= 0;
              }
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float min_val = stBlockInfo.min_max.float_val.nMin;

            if (filter->strategy == BTLessStrategyNumber)
            {
              bSatisfied = min_val < tar_val;
            }
            else 
            {
              bSatisfied = min_val <= tar_val;
            }
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int min_val = stBlockInfo.min_max.int_val.nMin;

            if (filter->strategy == BTLessStrategyNumber)
            {
              bSatisfied = min_val < tar_val;
            }
            else 
            {
              bSatisfied = min_val <= tar_val;
            }
          }
          
          break;
        }
        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        {
          // Datum   upper;

          // upper = getDatumForType(eType, stBlockInfo, false, false);
          // print_log(upper, eType, attname, val, filter->strategy);

          if (eType == Type_Str)
          {
            {
              int result = locale_compare(std::locale(), stBlockInfo.strMax, TextDatumGetCString(val));

              if (filter->strategy == BTGreaterStrategyNumber)
              {
                bSatisfied = result > 0;
              }
              else 
              {
                bSatisfied = result >= 0;
              }
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float max_val = stBlockInfo.min_max.float_val.nMax;

            if (filter->strategy == BTGreaterStrategyNumber)
            {
              bSatisfied = max_val > tar_val;
            }
            else 
            {
              bSatisfied = max_val >= tar_val;
            }
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int max_val = stBlockInfo.min_max.int_val.nMax;

            if (filter->strategy == BTGreaterStrategyNumber)
            {
              bSatisfied = max_val > tar_val;
            }
            else 
            {
              bSatisfied = max_val >= tar_val;
            }
          }

          break;
        }
        case BTEqualStrategyNumber:
        {
          // Datum   upper;
          // Datum   lower;

          // lower = getDatumForType(eType, stBlockInfo, true, false);
          // upper = getDatumForType(eType, stBlockInfo, false, false);

          // print_log(lower, eType, attname, val, filter->strategy);
          // print_log(upper, eType, attname, val, filter->strategy);

          if (eType == Type_Str)
          {
            size_t tar_len = filter->strVallen;
            // elog(LOG, "check len %d", tar_len);
            if (tar_len >= stBlockInfo.min_max.int_val.nMin &&
                tar_len <= stBlockInfo.min_max.int_val.nMax)
            {
              char *tar_val = TextDatumGetCString(val);
              int min_result = locale_compare(std::locale(), stBlockInfo.strMin, tar_val); 
              int max_result = locale_compare(std::locale(), stBlockInfo.strMax, tar_val); 

              bSatisfied = (min_result <= 0) && (max_result >= 0);
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float min_val = stBlockInfo.min_max.float_val.nMin;
            float max_val = stBlockInfo.min_max.float_val.nMax;

            bSatisfied = (min_val <= tar_val) && (max_val >= tar_val);
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int min_val = stBlockInfo.min_max.int_val.nMin;
            int max_val = stBlockInfo.min_max.int_val.nMax;

            bSatisfied = (min_val <= tar_val) && (max_val >= tar_val);
          }

          break;
        }
        default: // not equal case -> scan block condition
        {
          bSatisfied = true;
          break;
        }
      }
      if (!bSatisfied)
      {
        // elog(LOG, "scanned not satisfied: %d", nIdx);
        scanBlockInfo[nIdx] = false;
      }
    }
  }

  for (uint32 nIdx = 0; nIdx < nBlock; nIdx++)
  {
    if (scanBlockInfo[nIdx])
    {
      fdw_private->nTuple += rowCnt[nIdx];
      rowgroups = lappend_int(rowgroups, nIdx);
    }
  }

  pfree(scanBlockInfo);
  pfree(rowCnt);
  table_close(relation, AccessShareLock);

  // elog(LOG, "  tuple cost: %lf", fdw_private->nTuple);

  return rowgroups;
}

// void
// check_print()
// {
//   int result = locale_compare(std::locale(), "Incubator", "Incubator");

//   elog(LOG, " check string compare: %s, %d", result < 0 ? "true": "false", result);
// }

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  // Dog sDog("Plan");
  // elog(LOG, "db721_GetForeignRelSize: %s", sDog.Bark().c_str());

  db721PlanState *fdw_private;
  bool bFoundCond = false;

  // check_print();
  setlocale(LC_ALL, "en_US.UTF-8");
  std::locale::global(std::locale("en_US.utf8"));
  // check_print();

  fdw_private = (db721PlanState *) palloc0(sizeof(db721PlanState));
  db721GetOptions(foreigntableid,
                  &fdw_private->filename,
                  &fdw_private->tblname);
  baserel->fdw_private = (void *) fdw_private;

  fdw_private->nRawData = db721GetMeta(fdw_private->filename, &fdw_private->metadata);
  fdw_private->nBlock = clamp_row_est(fdw_private->nRawData / BLKSIZE);
  fdw_private->foreigntableid = foreigntableid;

  bFoundCond = extract_db721_filters(baserel->baserestrictinfo, fdw_private, baserel);
  fdw_private->columnList = getColumList(baserel, foreigntableid, fdw_private);
  
  while(1)
  {
    bool bResult = deleteElemClause(baserel->baserestrictinfo, fdw_private, baserel);

    if (bResult == false)
    {
      break;
    }
    else
    {
      // elog(LOG, " delete cell");
    }
  }

  if (baserel->baserestrictinfo != NIL)
  {
    elog(LOG, "check not deleted all");
    List *resultGroup = NIL;
    ListCell *lc;
    foreach(lc, fdw_private->columnList)
    {
      Var *var = (Var *) lfirst(lc);
      // elog(LOG, "check column info result %d", var->varattno);

      resultGroup = lappend_int(resultGroup, var->varattno);
    }
    fdw_private->relResultGroup = resultGroup;
  }

  if (bFoundCond)
  {
    fdw_private->relRowGroup = extract_db721_validblock(fdw_private, fdw_private->filters);
  }
  else
  {
    // scan all blocks
    List *rowgroups = NIL;
    rowgroups = lappend_int(rowgroups, -1);
    fdw_private->relRowGroup = rowgroups;
  }

  // double nTuple = getEstimatedTuple(baserel, fdw_private);
  // double rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo,
  //                                                0, JOIN_INNER, NULL);

  // baserel->rows = clamp_row_est(nTuple * rowSelectivity);
  baserel->rows = fdw_private->nTuple;
  // baserel->tuples = 2;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  // Dog sDog("Paths");
  // elog(LOG, "db721_GetForeignPaths: %s", sDog.Bark().c_str());

  // check if fdw_private released
  db721PlanState *fdw_private = (db721PlanState *) baserel->fdw_private;
  Path *scanPath = NULL;

  List *columnList = fdw_private->columnList;
  uint32 ncolumn = list_length(columnList);
  BlockNumber relationBlockCount = fdw_private->nBlock;
  uint32 nrelationColumn = getRelationCount(foreigntableid);

  // elog(LOG, "column: %d", ncolumn);

  double queryColumnRatio = (double) ncolumn / nrelationColumn;
  double queryBlockCount = relationBlockCount * queryColumnRatio;
  double totalFileBlockAccess = seq_page_cost * queryBlockCount;

  double tupleEstimate = getEstimatedTuple(baserel, fdw_private);

	double filterCostPerTuple = baserel->baserestrictcost.per_tuple;
	double cpuCostPerTuple = cpu_tuple_cost + filterCostPerTuple;
	double totalCpuCost = cpuCostPerTuple * tupleEstimate;

  double startupCost = baserel->baserestrictcost.startup;
  double totalCost = startupCost + totalCpuCost + totalFileBlockAccess;

  scanPath = (Path *) create_foreignscan_path(root, baserel,
                                              NULL,
                                              baserel->rows,
                                              startupCost, totalCost,
                                              NULL,
                                              baserel->lateral_relids,
                                              NULL, 
                                              NIL);  

  add_path(baserel, scanPath);
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  // Dog sDog("Plan");
  // elog(LOG, "db721_GetForeignPlan: %s", sDog.Bark().c_str());

  db721PlanState *fdw_private = (db721PlanState *) baserel->fdw_private;
  List *columnList = NIL;
  List *foreignPrivateList = NIL;
  List *params = NIL;

  scan_clauses = extract_actual_clauses(scan_clauses, false);
  // List *filters = NIL;
  // extract_db721_filters(scan_clauses, filters, baserel);

  columnList = fdw_private->columnList;
  foreignPrivateList = list_make1(columnList);

  params = lappend(params, fdw_private->relRowGroup);
  params = lappend(params, fdw_private->relResultGroup);
  params = lappend(params, fdw_private->filters);
  params = lappend(params, foreignPrivateList);

  return make_foreignscan(tlist,
                          scan_clauses,
                          baserel->relid,
                          NIL,
                          params,
                          NIL,    /* no custom tlist */
                          NIL,    /* no remote quals */
                          outer_plan);
}

BlockBufferInfo *
allocBlockBufferInfo(uint32 nSize, DataType eType)
{
  BlockBufferInfo *res = NULL;
  uint32 nBufferSize;
  uint32 nElementSize = 4;

  switch (eType)
  {
    case Type_Str:
      nElementSize = 32;
      break;
    case Type_int:
      nElementSize = sizeof(float);
      break;
    case Type_float:
      nElementSize = sizeof(int);
      break;
    default:
      // error
      break;
  }
  nBufferSize = nSize * nElementSize;

  // elog(LOG, "buffersize: %d", nBufferSize);

  res = (BlockBufferInfo *) palloc0(sizeof(BlockBufferInfo));
  res->data = (char *) palloc0(nBufferSize + VARHDRSZ);
  res->eType = eType;
  res->ntypeSize = nElementSize;
  res->ncursor = 0;

  return res;
}

static ColumnBufferMgr *
allocBufferManager(uint32 nColumn, List *columnList, json *metadata, TupleDesc *tupleDesc)
{
  ColumnBufferMgr *columnBufMgr = NULL;
  bool *columnMask = (bool *) palloc0(nColumn * sizeof(bool));
  ListCell *columnCell = NULL;
  uint32 columnIdx;
  uint32 nMaxCountInBlk = 0;

  nMaxCountInBlk = (*metadata)["Max Values Per Block"].get<int>();

  columnBufMgr = (ColumnBufferMgr *) palloc0(sizeof(ColumnBufferMgr));

  columnBufMgr->nColumn = nColumn;
  columnBufMgr->blockBufferInfoArray = (BlockBufferInfo **) palloc0(nColumn * sizeof(BlockBufferInfo *));
  columnBufMgr->nMaxValueInBlock = nMaxCountInBlk;

  // for (json::iterator it = (*metadata).begin(); it != (*metadata).end(); ++it)
  // {
  //   elog(LOG, "    parsing iterator: %s, value: %s", it.key().c_str(), it.value());    
  // }

  // elog(LOG, "    parsing value: %s", (*metadata)["Table"].get<std::string>().c_str());
  // elog(LOG, "    parsing value: %d", (*metadata)["Max Values Per Block"].get<int>());

  foreach (columnCell, columnList)
  {
    Var *column = (Var *) lfirst(columnCell);
    columnIdx = column->varattno - 1;
    Form_pg_attribute attr = TupleDescAttr(*tupleDesc, columnIdx);
    char *attname = NameStr(attr->attname);
    DataType eType;
    BlockBufferInfo *blockBufferInfo = NULL;
    json curColumn;

    // elog(LOG, "    parsing column : %d, %s", columnIdx, attname);
    
    columnMask[columnIdx] = true;
    
    curColumn = (*metadata)["Columns"][attname];
    eType = getDataType(curColumn["type"].get<std::string>().c_str());
    blockBufferInfo = allocBlockBufferInfo(nMaxCountInBlk, eType);
    blockBufferInfo->nBlock = curColumn["num_blocks"].get<int>();
    blockBufferInfo->nBlockIdx = 0;
    blockBufferInfo->nStartOffset = curColumn["start_offset"].get<int>();
    blockBufferInfo->attname = attname; // TODO: check released
    blockBufferInfo->bLoaded = false;

    columnBufMgr->nBlock = blockBufferInfo->nBlock;
    columnBufMgr->blockBufferInfoArray[columnIdx] = blockBufferInfo;
  }

  columnBufMgr->scanInfoArr = (bool *) palloc0(columnBufMgr->nBlock * sizeof(bool));

  return columnBufMgr;
}

static void
db721BeginRead(db721ScanState *fdw_private, ForeignScanState *node)
{
  Relation currelation = node->ss.ss_currentRelation;
  TupleDesc tupleDesc = RelationGetDescr(currelation);
  List *foreignPrivateList = NIL;
  List *whereClauseList = NIL;
  List *columnList = NIL;
  List *relRowGroup = NIL;
  List *relResultGroup = NIL;
  uint32 nColumn = 0;
  uint32 nIdx = 0;

  ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
  List *params = (List *) foreignScan->fdw_private;
  ListCell *paramCell = NULL;
  foreach (paramCell, params)
  {
    switch (nIdx)
    {
      case 0: // relrowgroup
        relRowGroup = (List *) lfirst(paramCell);
        fdw_private->rowgroup = relRowGroup;
        break;
      case 1: // relresultgroup
        relResultGroup = (List *) lfirst(paramCell);
        break;
      case 2: // filter
        fdw_private->filters = (List *) lfirst(paramCell);
        break;
      case 3: // privateList
        foreignPrivateList = (List *) lfirst(paramCell);
        break;
    }
    nIdx++;
  }

  if (fdw_private->rowgroup == NIL)
  {
    // elog(LOG, "  no scan data!");
    return;
  }

  Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
  db721GetOptions(foreigntableid,
                  &fdw_private->filename,
                  &fdw_private->tblname);

  db721GetMeta(fdw_private->filename, &fdw_private->metadata);

  whereClauseList = foreignScan->scan.plan.qual;

  columnList = (List *) linitial(foreignPrivateList);
  nColumn = tupleDesc->natts;

  fdw_private->pFile = fopen(fdw_private->filename, "r");  
  fdw_private->coulmnBufferMgr = allocBufferManager(nColumn, columnList, &fdw_private->metadata, &tupleDesc);
  fdw_private->tupleDesc = tupleDesc;
  fdw_private->columnList = columnList;
  fdw_private->whereClauseList = whereClauseList;
  fdw_private->checkResult = (bool *) palloc0(nColumn * sizeof(bool));

  ListCell *rowCell = NULL;
  foreach (rowCell, relRowGroup)
  {
    int nIdx = lfirst_int(rowCell);

    // elog(LOG, "scanned idx: %d", nIdx);
    if (nIdx == -1)
    {
      // elog(LOG, "no condition catched: %d", fdw_private->coulmnBufferMgr->nBlock);
      int nBlock = fdw_private->coulmnBufferMgr->nBlock;
      for (nIdx = 0; nIdx < nBlock; nIdx++)
      {
        fdw_private->coulmnBufferMgr->scanInfoArr[nIdx] = true;
      }
    }
    else
    {
      fdw_private->coulmnBufferMgr->scanInfoArr[nIdx] = true;
    }
  }

  ListCell *resultCell = NULL;
  foreach (resultCell, relResultGroup)
  {
    int nIdx = lfirst_int(resultCell) - 1;

    fdw_private->checkResult[nIdx] = true;
  }
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
  // Dog sDog("Scan");
  // elog(LOG, "db721_BeginForeignScan: %s", sDog.Bark().c_str());

  db721ScanState *fdw_private = NULL;

  if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
  {
    return;
  }

  fdw_private = (db721ScanState *) palloc0(sizeof(db721ScanState));

  db721BeginRead(fdw_private, node);
  node->fdw_state = (void *) fdw_private;
}

bool
ReadNextRow(db721ScanState *fdw_private, Datum *columnValues, bool *columnNulls)
{
  ColumnBufferMgr *BufferMgr = fdw_private->coulmnBufferMgr;
  List *columnList = fdw_private->columnList;
  ListCell *columnCell = NULL;
  bool bFound = true;

  foreach (columnCell, columnList)
  {
    Var *columnVar = (Var *) lfirst(columnCell);
    uint32 columnIdx = columnVar->varattno - 1;
    BlockBufferInfo *curBufferInfo = BufferMgr->blockBufferInfoArray[columnIdx];
    char *curValue;

    if (curBufferInfo == NULL)
    {
      elog(LOG, "error check col index: %d", columnIdx);
    }

    if (curBufferInfo->bLoaded == false)
    {
      // elog(LOG, "end scan block index: %d", curBufferInfo->nBlockIdx);
      bFound = false;
      break;
    }

    uint32 nOffset = curBufferInfo->ncursor;

    curBufferInfo->ncursor += curBufferInfo->ntypeSize;
    if (curBufferInfo->ncursor == curBufferInfo->nBufferSize)
    {
      // elog(LOG, "error check col index: %d", curBufferInfo->ncurItemCnt);
      curBufferInfo->bLoaded = false;
    }

    if (bFound == false)
    {
      continue;
    }

    if (curBufferInfo->eType == Type_Str)
    {
      curValue = curBufferInfo->data + nOffset;
      if (fdw_private->checkResult[columnIdx])
      {
        columnValues[columnIdx] = CStringGetTextDatum(curValue);
        columnNulls[columnIdx] = false;
      }
    }
    else
    {
      curValue = curBufferInfo->data + nOffset;

      if (fdw_private->checkResult[columnIdx])
      {
        columnNulls[columnIdx] = false;
        if (curBufferInfo->eType == Type_float)
        {
          columnValues[columnIdx] = Float4GetDatum(*(float *)curValue);
        } else {
          columnValues[columnIdx] = Int32GetDatum(*(int *)curValue);
        }
      }
    }

    ListCell *filterCell = NULL;
    DataType eType = curBufferInfo->eType;
    foreach (filterCell, fdw_private->filters)
    {
      db721Filter *filter = (db721Filter *) lfirst(filterCell);
      uint32 filterIdx = filter->attnum - 1;
      if (filterIdx != columnIdx)
      {
        continue;
      }

      Datum val = filter->value->constvalue;
      bool bSatisfied = false;
      
      switch (filter->strategy)
      {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
        {
          if (eType == Type_Str)
          {
            int result = locale_compare(std::locale(), curValue, TextDatumGetCString(val));
            if (filter->strategy == BTLessStrategyNumber)
            {
              bSatisfied = result < 0;
            }
            else
            {
              bSatisfied = result <= 0;
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float cur_val = *(float *)curValue;

            if (filter->strategy == BTLessStrategyNumber)
            {
              bSatisfied = cur_val < tar_val;
            }
            else
            {
              bSatisfied = cur_val <= tar_val;
            }
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int cur_val = *(int *)curValue;

            if (filter->strategy == BTLessStrategyNumber)
            {
              bSatisfied = cur_val < tar_val;
            }
            else
            {
              bSatisfied = cur_val <= tar_val;
            }
          }

          break;
        }       
        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
        {
          if (eType == Type_Str)
          {
            int result = locale_compare(std::locale(), curValue, TextDatumGetCString(val));
            if (filter->strategy == BTGreaterStrategyNumber)
            {
              bSatisfied = result > 0;
            }
            else
            {
              bSatisfied = result >= 0;
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float cur_val = *(float *)curValue;

            if (filter->strategy == BTGreaterStrategyNumber)
            {
              bSatisfied = cur_val > tar_val;
            }
            else
            {
              bSatisfied = cur_val >= tar_val;
            }
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int cur_val = *(int *)curValue;

            if (filter->strategy == BTGreaterStrategyNumber)
            {
              bSatisfied = cur_val > tar_val;
            }
            else
            {
              bSatisfied = cur_val >= tar_val;
            }
          }

          break;
        }
        case BTEqualStrategyNumber:
        {
          if (eType == Type_Str)
          {
            int tar_len = filter->strVallen;
            int cur_len = strlen(curValue);

            if (tar_len == cur_len)
            {
              int result = locale_compare(std::locale(), curValue, TextDatumGetCString(val));
              bSatisfied = (result == 0);
            }
            else
            {
              bSatisfied = false;
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float cur_val = *(float *)curValue;

            bSatisfied = (cur_val == tar_val);
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int cur_val = *(int *)curValue;

            bSatisfied = (cur_val == tar_val);
          }

          break;
        }
        default:
        {
          if (eType == Type_Str)
          {
            int tar_len = filter->strVallen;
            int cur_len = strlen(curValue);

            if (tar_len == cur_len)
            {
              int result = locale_compare(std::locale(), curValue, TextDatumGetCString(val));
              bSatisfied = (result != 0);
            }
            else
            {
              bSatisfied = true;
            }
          }
          else if (eType == Type_float)
          {
            float tar_val = DatumGetFloat8(val);
            float cur_val = *(float *)curValue;

            bSatisfied = (cur_val != tar_val);
          }
          else
          {
            int tar_val = DatumGetInt32(val);
            int cur_val = *(int *)curValue;

            bSatisfied = (cur_val != tar_val);
          }

          break;
        }
      }

      if (!bSatisfied)
      {
        bFound = false;
      }
    }
  }

  return bFound;
}

bool
checknLoad(db721ScanState *fdw_private, uint32 nColumn)
{
  ColumnBufferMgr *bufferMgr = fdw_private->coulmnBufferMgr;
  uint32 nIdx;
  FILE *pFile = fdw_private->pFile;
  bool bLast = false;

  for (nIdx = 0; nIdx < nColumn; nIdx++)
  {
    BlockBufferInfo *curBufferInfo = bufferMgr->blockBufferInfoArray[nIdx];
    json blockStat;
    uint32 nBlockIdx;
    std::string idxChar;
    uint32 nBufferSize;
    uint32 nOffset;
    bool bFound = false;

    if (curBufferInfo == NULL)
    {
      continue;
    }

    if (curBufferInfo->bLoaded)
    {
      continue;
    }

    if (curBufferInfo->nBlock == curBufferInfo->nBlockIdx)
    {
      bLast = true;
      continue;
    }

    while(1)
    {
      nBlockIdx = curBufferInfo->nBlockIdx;
      if (nBlockIdx == curBufferInfo->nBlock)
      {
        bLast = true;
        // elog(LOG, "no more scan: %s", curBufferInfo->attname);
        break;
      }
      curBufferInfo->nBlockIdx++;
      idxChar = std::to_string(nBlockIdx);
      curBufferInfo->ncursor = 0;
      blockStat = (fdw_private->metadata)["Columns"][curBufferInfo->attname]["block_stats"][idxChar.c_str()];
      
      nBufferSize = (blockStat["num"].get<int>()) * curBufferInfo->ntypeSize;
      curBufferInfo->nBufferSize = nBufferSize;
      nOffset = curBufferInfo->nStartOffset;
      curBufferInfo->nStartOffset += nBufferSize;

      if (fdw_private->coulmnBufferMgr->scanInfoArr[nBlockIdx])
      {
        // elog(LOG, "load column idx: %d, colname: %s", nBlockIdx, curBufferInfo->attname);
        bFound = true;
        break;
      }
      else
      {
        // elog(LOG, "skip column idx: %d, colname: %s", nBlockIdx, curBufferInfo->attname);
      }
    }

    if (bFound)
    {
      curBufferInfo->bLoaded = true;
      fseek(pFile, nOffset, SEEK_SET);
      // fgets(curBufferInfo->data, nBufferSize, pFile);
      for (uint32 nIdx = 0; nIdx < nBufferSize; nIdx++)
      {
        curBufferInfo->data[nIdx] = fgetc(pFile);
      }
    }
  }

  return bLast;
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  // Dog sDog("Plan");
  // elog(LOG, "db721_IterateForeignScan: %s", sDog.Bark().c_str());

  db721ScanState *fdw_private = (db721ScanState *) node->fdw_state;
  TupleTableSlot *tupleslot = node->ss.ss_ScanTupleSlot;
  bool    found = false;

  TupleDesc tupleDesc = tupleslot->tts_tupleDescriptor;
  Datum *columnValues = tupleslot->tts_values;
  bool  *columnNulls = tupleslot->tts_isnull;
  uint32  nColumn = tupleDesc->natts;
  bool bLast = false;

  memset(columnValues, 0, nColumn * sizeof(Datum));
  memset(columnNulls, true, nColumn * sizeof(bool));

  ExecClearTuple(tupleslot);

  if (fdw_private->rowgroup == NIL)
  {
    // elog(LOG, "  no scan data iterate!");
    return tupleslot;
  }

  while(1)
  {
    bLast = checknLoad(fdw_private, nColumn);

    if(bLast)
    {
      found = false;
      break;
    }

    found = ReadNextRow(fdw_private, columnValues, columnNulls);

    if (found)
    {
      break;
    }
  }

  if (found)
  {
    ExecStoreVirtualTuple(tupleslot);
  }

  return tupleslot;
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  // Dog sDog("Plan");
  // elog(LOG, "db721_EndForeignScan: %s", sDog.Bark().c_str());

  db721ScanState *fdw_private = (db721ScanState *) node->fdw_state;
  
  if (fdw_private != NULL)
  {
    if (fdw_private->rowgroup == NIL)
    {
      // elog(LOG, "  no scan data end!");
      pfree(fdw_private);
      return;
    }

    ColumnBufferMgr *bufferMgr = fdw_private->coulmnBufferMgr;
    uint32 nColumn = bufferMgr->nColumn;
    uint32 nIdx;

    for(nIdx = 0; nIdx < nColumn; nIdx++)
    {
      BlockBufferInfo *bufferInfo = bufferMgr->blockBufferInfoArray[nIdx];

      if (bufferInfo != NULL)
      {
        pfree(bufferInfo->data);
        pfree(bufferInfo);
      }
    }

    pfree(bufferMgr->scanInfoArr);
    pfree(bufferMgr->blockBufferInfoArray);
    pfree(fdw_private->coulmnBufferMgr);
    fclose(fdw_private->pFile);
    pfree(fdw_private);
  }
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  // Dog sDog("Plan");
  // elog(LOG, "db721_ReScanForeignScan: %s", sDog.Bark().c_str());

  db721_EndForeignScan(node);
  db721_BeginForeignScan(node, 0);
}