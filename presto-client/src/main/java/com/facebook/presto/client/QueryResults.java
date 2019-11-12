/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

import com.facebook.presto.spi.PrestoWarning;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.client.FixJsonDataUtils.fixData;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryResults
        implements QueryStatusInfo, QueryData
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;
    private final StatementStats stats;
    private final QueryError error;
    private final List<PrestoWarning> warnings;
    private final String updateType;
    private final Long updateCount;



    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("warnings") List<PrestoWarning> warnings,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this(
                id,
                infoUri,
                partialCancelUri,
                nextUri,
                columns,
                fixData(columns, data),
                stats,
                error,
                firstNonNull(warnings, ImmutableList.of()),
                updateType,
                updateCount);
    }

    public QueryResults(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Column> columns,
            Iterable<List<Object>> data,
            StatementStats stats,
            QueryError error,
            List<PrestoWarning> warnings,
            String updateType,
            Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
        checkArgument(data == null || columns != null, "data present without columns");
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.warnings = ImmutableList.copyOf(requireNonNull(warnings, "warnings is null"));
        this.updateType = updateType;
        this.updateCount = updateCount;

        System.out.println(this.toString());
    }

    @JsonProperty
    @Override
    public String getId()
    {
        return id;
    }

    @JsonProperty
    @Override
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    @Nullable
    @JsonProperty
    @Override
    public Iterable<List<Object>> getData()
    {
        return data;
    }

    @JsonProperty
    @Override
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    @Override
    public QueryError getError()
    {
        return error;
    }

    @JsonProperty
    @Override
    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    @Nullable
    @JsonProperty
    @Override
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    @Override
    public Long getUpdateCount()
    {
        return updateCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("\nid", id)
                .add("\ninfoUri", infoUri)
                .add("\npartialCancelUri", partialCancelUri)
                .add("\nnextUri", nextUri)
//                .add("columns", columns)
                .add("\ncolumns", this.columnsToString(columns))
                .add("\nhasData", data != null)
                .add("\ndata", this.dataToString(data))
                .add("\nstats", stats)
                .add("\nerror", error)
                .add("\nupdateType", updateType)
                .add("\nupdateCount", updateCount)
                .toString();
    }

    /***
     * 字段转字符串
     * @param columns
     * @return
     */
    private String columnsToString(List<Column> columns){

        String columnStr = "\n";
        if(columns!=null){
            for (Column column: columns ) {
                String str = column.getName()+"/"+column.getType()+";";
                columnStr = columnStr + str;
            }
        }
        return columnStr;
    }


    /***
     * 通过System.out.println的方式将QueryResults中的数据输出
     * @param queryResults
     */
    private String dataToString(Iterable<List<Object>> data){

        String dataStr = "\n";
        if(data!=null){
            Iterator<List<Object>> iterator = data.iterator();
            while (iterator.hasNext()){
                List<Object> list = iterator.next();
                if(list!=null){
                    for(Object obj : list){
                        String str = String.valueOf(obj) + "/";
                        dataStr = dataStr + str;
                    }
                    dataStr = dataStr + "\n";
                }
            }

        }

        return dataStr;

    }


}
