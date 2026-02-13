package com.gpb.metadata.ingestion.utils;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;

import org.springframework.stereotype.Component;

import java.util.*;

@Component
public final class ViewSqlLineageParser {

    public record TableRef(String schema, String name) {}
    public record ColumnRef(String tableAliasOrName, String column) {}
    public record ColumnMapping(String toColumn, Set<ColumnRef> from) {}

    public record ParsedLineage(
            Map<String, TableRef> aliasToTable,   // alias/name -> (schema,table)
            Set<TableRef> upstreamTables,         // tables from FROM/JOIN
            List<ColumnMapping> columnMappings    // view col -> upstream cols
    ) {}

    public ParsedLineage parse(String viewDefinition) {
        if (viewDefinition == null || viewDefinition.isBlank()) {
            return new ParsedLineage(Map.of(), Set.of(), List.of());
        }

        try {
            Statement stmt = CCJSqlParserUtil.parse(viewDefinition);
            if (!(stmt instanceof Select sel)) {
                return new ParsedLineage(Map.of(), Set.of(), List.of());
            }

            var body = sel.getSelectBody();
            if (!(body instanceof PlainSelect ps)) {
                return new ParsedLineage(Map.of(), Set.of(), List.of());
            }

            Map<String, TableRef> aliasToTable = extractAliasToTable(ps);
            Set<TableRef> upstream = new LinkedHashSet<>(aliasToTable.values());
            List<ColumnMapping> mappings = extractColumnMappings(ps);

            return new ParsedLineage(aliasToTable, upstream, mappings);

        } catch (Exception e) {
            return new ParsedLineage(Map.of(), Set.of(), List.of());
        }
    }

    private static Map<String, TableRef> extractAliasToTable(PlainSelect ps) {
        Map<String, TableRef> out = new LinkedHashMap<>();

        addFromItem(ps.getFromItem(), out);

        List<Join> joins = ps.getJoins();
        if (joins != null) {
            for (Join j : joins) {
                addFromItem(j.getRightItem(), out);
            }
        }

        return out;
    }

    private static void addFromItem(FromItem fi, Map<String, TableRef> out) {
        if (fi == null) return;

        if (fi instanceof Table t) {
            String schema = t.getSchemaName(); // nullable
            String name = t.getName();
            String alias = (t.getAlias() != null) ? t.getAlias().getName() : null;

            TableRef ref = new TableRef(schema, name);

            if (alias != null && !alias.isBlank()) out.put(alias, ref);
            if (name != null && !name.isBlank()) out.put(name, ref);
        }

        // SubSelect / Values / Function пока игнорируем (best effort)
    }

    private static List<ColumnMapping> extractColumnMappings(PlainSelect ps) {
        List<ColumnMapping> out = new ArrayList<>();

        List<SelectItem<?>> items = ps.getSelectItems();
        if (items == null) return out;

        for (SelectItem<?> item : items) {
            Expression expr = item.getExpression();

            // пропускаем * и t.*
            if (expr instanceof AllColumns || expr instanceof AllTableColumns) {
                continue;
            }

            // имя целевой колонки view
            String toCol = (item.getAlias() != null) ? item.getAlias().getName() : null;

            // если alias нет — и expr = Column, можно взять имя колонки
            if ((toCol == null || toCol.isBlank()) && expr instanceof Column c) {
                toCol = c.getColumnName();
            }

            if (toCol == null || toCol.isBlank()) {
                // выражение без alias (например concat(a,b)) — пропускаем
                continue;
            }

            Set<ColumnRef> fromCols = new LinkedHashSet<>();
            collectColumnRefs(expr, fromCols);

            if (!fromCols.isEmpty()) {
                out.add(new ColumnMapping(toCol, fromCols));
            }
        }

        return out;
    }

    private static void collectColumnRefs(Expression expr, Set<ColumnRef> out) {
        if (expr == null) return;

        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                String table = (column.getTable() != null) ? column.getTable().getName() : null;
                String col = column.getColumnName();
                if (col != null && !col.isBlank()) {
                    out.add(new ColumnRef(table, col));
                }
            }
        });
    }
}