---
title: Asset Performance
---

<ButtonGroup name=platform>
    <ButtonGroupItem valueLabel="TikTok" value = "tt_ads_report" default/>
    <ButtonGroupItem valueLabel="Facebook" value = "fb_ads_report" />
</ButtonGroup>

<DateRange
    name=key_metrics_date_range
    data={key_metrics_by_month}
    dates=Date
    defaultValue={'Last 7 Days'}
/>

<Dropdown
    data={ad_names_for_dropdown_data_table}
    name=ad_name_data_table
    value=creative_name
    multiple=true
    selectAllByDefault=true
    title="Select Ads"
/>

<Modal title="Formulas" buttonText='Formulas'>

| Metric      | Formula |
| :---:        |    :----:   |
| ROI      | (revenue/amount_spent)-1       |
| CTR   | clicks/impressions        |
| CPM      | (amount_spent/impressions)*1000       |
| eCPM   | (revenue/impressions)*1000        |
| CPC      | amount_spent/clicks       |
| CPA   | revenue/conversions_tracker        |
| RPC      | revenue/clicks       |
| ARPU   | revenue/conversions_tracker        |

</Modal>

<DataTable data={key_metrics_by_month_for_data_table}
    groupBy=creative_name
    subtotals=true
    groupsOpen=false
    rowShading=true 
    sortable=true 
    rows=5 
    totalRow=true
    totalRowColor=#fff0cc
    compact=true >
  <Column id=Date totalAgg=""/>  
 	<Column id=roi_pct title="ROI" totalAgg=weightedMean weightCol=spent_usd redNegatives=true contentType=delta deltaSymbol = false/> 
	<Column id=ctr_pct title = "CTR" totalAgg=weightedMean weightCol=impressions contentType=colorscale scaleColor={['#ce5050','white','#6db678']} colorMid=0.01/>
	<Column id=cpm_usd title="CPM" totalAgg=weightedMean weightCol=impressions contentType=colorscale scaleColor={['#6db678','white','#ce5050']} colorMid={(key_metrics_by_month_for_data_table.map(item => item.cpm_usd).reduce((sum, value) => sum + value, 0))/(key_metrics_by_month_for_data_table.map(item => item.cpm_usd).length)}/>
	<Column id=ecpm_usd title="eCPM" totalAgg=weightedMean weightCol=impressions contentType=colorscale scaleColor={['#ce5050','white','#6db678']} colorMid={(key_metrics_by_month_for_data_table.map(item => item.ecpm_usd).reduce((sum, value) => sum + value, 0))/(key_metrics_by_month_for_data_table.map(item => item.ecpm_usd).length)}/>
	<Column id=cpc_usd title="CPC" totalAgg=weightedMean weightCol=clicks contentType=colorscale scaleColor={['#6db678','white','#ce5050']} colorMid=0.05/>
	<Column id=cpa_usd title="CPA" totalAgg=weightedMean weightCol=conversions_tracker contentType=colorscale scaleColor={['#6db678','white','#ce5050']} colorMid={(key_metrics_by_month_for_data_table.map(item => item.cpa_usd).reduce((sum, value) => sum + value, 0))/(key_metrics_by_month_for_data_table.map(item => item.cpa_usd).length)}/>
	<Column id=rpc_usd title="RPClick" totalAgg=weightedMean weightCol=clicks contentType=colorscale scaleColor={['#ce5050','white','#6db678']} colorMid={(key_metrics_by_month_for_data_table.map(item => item.rpc_usd).reduce((sum, value) => sum + value, 0))/(key_metrics_by_month_for_data_table.map(item => item.rpc_usd).length)}/>
	<Column id=arpu_usd title="RPConversion" totalAgg=weightedMean weightCol=conversions_tracker contentType=colorscale scaleColor={['#ce5050','white','#6db678']} colorMid={(key_metrics_by_month_for_data_table.map(item => item.arpu_usd).reduce((sum, value) => sum + value, 0))/(key_metrics_by_month_for_data_table.map(item => item.arpu_usd).length)}/>
  <Column id=spent_usd title="Spent" totalAgg=sum/> 
  <Column id=revenue_usd title="Revenue" totalAgg=sum/> 
  <Column id=impressions totalAgg=sum fmt='#,###'/> 
  <Column id=clicks totalAgg=sum fmt='#,###'/> 
  <Column id=reach totalAgg=sum fmt='#,###'/> 
  <Column id=conversions_tracker title = "Conversions (TONIC)" totalAgg=sum fmt='#,###'/>
</DataTable>

<DateRange
    name=key_metrics_date_range_line_chart
    data={key_metrics_by_month}
    dates=Date
    defaultValue={'Last 7 Days'}
/>

<Dropdown
    data={ad_names_for_dropdown_line_chart}
    name=ad_name_line_chart
    value=creative_name
    multiple=true
    selectAllByDefault=true
    title="Select Ads"
/>

<LineChart 
    data={key_metrics_by_month_for_line_chart}
    x=Date
    y={['cpc_usd','rpc_usd']}
    y2=roi_pct
    y2SeriesType=bar
    yAxisTitle="CPC, RPC"
    y2AxisTitle="ROI"
    labels=true
    yLabelFmt = '$#,##0.000'
    y2LabelFmt='#,##0.0%'
    labelSize=12
    colorPalette={['#eba646','#46e3d6','#00000']}
    chartAreaHeight=400
    yFmt='$#,##0.000'
    y2Fmt='#,##0.0%'
    echartsOptions={{
    legend: {
        orient: 'horizontal',
        top: 0,
        align: 'auto',
        show: true,
        data: [
         "Cpc Usd",
         "Rpc Usd"
      ],
        formatter: function (name) {
          const titles = {
           "Roi Pct": "ROI",
           "Cpc Usd": "CPC",
           "Rpc Usd": "RPC",
          };
          return titles[name] || name;
        }
    },
    grid: {
        top: '50px'
    },
    yAxis: [
      {
      axisLabel: {
        color: '#000000'
        },
      nameTextStyle: {
          color: '#000000'
        }
      },
      {
      axisLabel: {
        color: '#00000'
      },
      nameTextStyle: {
          color: '#000000'
        }
      }
   ],
   series: [
    {},
    {},
    {
      itemStyle: {
        color: function(params){
          return params.value[1] <= 0 ? '#c93328' : '#31a324';
        }
      }
    }
   ]
}}
/>


```sql ad_names_for_dropdown_line_chart
  select
  creative_name
  FROM  ${key_metrics_by_month}
  WHERE Date between DATE '${inputs.key_metrics_date_range_line_chart.start}' + 1 and DATE '${inputs.key_metrics_date_range_line_chart.end}' + 1
  GROUP BY date, creative_name
  ORDER BY date DESC
```

```sql key_metrics_by_month_for_line_chart
  select
  Date as Date,
  SUM(cpc_usd*clicks)/SUM(clicks) AS cpc_usd,
  SUM(rpc_usd*clicks)/SUM(clicks) AS rpc_usd,
  SUM(roi_pct*spent_usd)/SUM(spent_usd) AS roi_pct
  FROM  ${key_metrics_by_month}
  WHERE Date between DATE '${inputs.key_metrics_date_range_line_chart.start}' + 1 and DATE '${inputs.key_metrics_date_range_line_chart.end}' + 1
  and creative_name in ${inputs.ad_name_line_chart.value}
  GROUP BY date
  ORDER BY date DESC
```

```sql ad_names_for_dropdown_data_table
  select creative_name
  FROM  ${key_metrics_by_month}
  WHERE 
  Date between DATE '${inputs.key_metrics_date_range.start}' + 1
  and DATE '${inputs.key_metrics_date_range.end}' + 1
  ORDER BY date DESC
```

```sql key_metrics_by_month_for_data_table
  select * 
  FROM  ${key_metrics_by_month}
  WHERE 
  Date between DATE '${inputs.key_metrics_date_range.start}' + 1
  and DATE '${inputs.key_metrics_date_range.end}' + 1
  and creative_name in ${inputs.ad_name_data_table.value}

  ORDER BY date DESC
```

```sql key_metrics_by_month
  select
      ad_name AS creative_name,
      date AS Date,
      (revenue/amount_spent)-1 AS roi_pct,
      (clicks/impressions) AS ctr_pct,
      (amount_spent/impressions)*1000 AS cpm_usd,
      (revenue/impressions)*1000 AS ecpm_usd,
      amount_spent/clicks AS cpc_usd,
      amount_spent/conversions_tracker AS cpa_usd,
      revenue/clicks AS rpc_usd,
      revenue/conversions_tracker AS arpu_usd,
      amount_spent as spent_usd,
      revenue AS revenue_usd,
      impressions AS impressions,
      clicks AS clicks,
      reach AS reach,
      conversions_tracker AS conversions_tracker
  from ${inputs.platform}
  ORDER BY date DESC
```
