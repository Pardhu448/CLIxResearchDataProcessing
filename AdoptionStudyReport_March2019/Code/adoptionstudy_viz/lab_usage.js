
$( document ).ready(function() {
    render_chart(csv_data = "data/sirohi_Jul-Dec-2018_lab_func.csv");
 });

function render_chart(csv_data){

var margin = {top: 20, right: 20, bottom: 100, left: 40},
width = 960 - margin.left - margin.right,
height = 500 - margin.top - margin.bottom;

var x = d3.scale.ordinal()
    .rangeRoundBands([0, width], .1);

var y = d3.scale.linear()
    .rangeRound([height, 0]);

var color = d3.scale.ordinal()
    .range(["#DC143C", "#DEB887", "#8FBC8F", "#ADD8E6", "#FF4500"])

// var color = d3.scale.linear()
//               .range(["#3366cc", "#dc3912", "#ff9900", "#109618",
//               "#990099", "#0099c6", "#dd4477", "#66aa00", "#b82e2e", "#316395"]);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom");

var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .tickFormat(d3.format(".2s"));

var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var active_link = "0"; //to control legend selections and hover
var legendClicked; //to control legend selections
var legendClassArray = []; //store legend classes to select bars in plotSingle()
var y_orig; //to store original y-posn



d3.csv(csv_data, function(error, data) {
  if (error) throw error;
  color.domain(d3.keys(data[0]).filter(function(key) { return key !== "school_server_code"; }));

  data.forEach(function(d) {
    var mystate = d.school_server_code; //add to stock code
    var y0 = 0;
    //d.ages = color.domain().map(function(name) { return {name: name, y0: y0, y1: y0 += +d[name]}; });
    d.ages = color.domain().map(function(name) { return {mystate:mystate, name: name, y0: y0, y1: y0 += +d[name]}; });
    d.total = d.ages[d.ages.length - 1].y1;
  });

  data.sort(function(a, b) { return b.total - a.total; });

  x.domain(data.map(function(d) { return d.school_server_code; }));
  y.domain([0, d3.max(data, function(d) { return d.total; })]);

  var maxWidth = 0

  svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
      .selectAll("text")
         .remove()
//         .each(function () {
//		        var boxWidth = this.getBBox().width;
//		        if (boxWidth > maxWidth) maxWidth = boxWidth;
//	            })
//	     .attr("transform", "translate(0," + (height - maxWidth) + ")")
//        .attr("y", 0)
//         .attr("x", 9)
//         .attr("dy", ".55em")
//         .attr("transform", "rotate(90)")
//         .style("text-anchor", "start");


svg.append("text")
      .attr("transform",
            "translate(" + (width/30) + " ," +
                           (height + margin.top + 50) + ")")
      .style("text-anchor", "right")
      .text("*Only logged-in and buddy users are included, anonymous users are not included");

  svg.append('text')
      .attr("x", 390)
      .attr("y",   50)
      .style("text-anchor", "middle")
      .text("Number of Days Lab Functional across All States (Jul, 2018 - Mar, 2019)")
      .style("font-size", "18px")
      .style("fill", 'black')

  svg.append('text')
      .attr("x", 390)
      .attr("y",   80)
      .style("text-anchor", "middle")
      .text("")
      .style("font-size", "18px")
      .style("fill", 'black')

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end");

  svg.append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 0 - margin.left)
      .attr("x",0 - (height / 2))
      .attr("dy", ".7em")
      .style("text-anchor", "middle")
      .text("Number of Days");

  var state = svg.selectAll(".state")
      .data(data)
    .enter().append("g")
      .attr("class", "g")
      .attr("transform", function(d) { return "translate(" + "0" + ",0)"; });
      //.attr("transform", function(d) { return "translate(" + x(d.State) + ",0)"; })

  state.selectAll("rect")
      .data(function(d) {
        return d.ages;
      })
    .enter().append("rect")
      .attr("width", x.rangeBand())
      .attr("y", function(d) { return y(d.y1); })
      .attr("x",function(d) { //add to stock code
          return x(d.mystate)
        })
      .attr("height", function(d) { return y(d.y0) - y(d.y1); })
      .attr("class", function(d) {
        classLabel = d.name.replace(/\s/g, ''); //remove spaces
        return "class" + classLabel;
      })
      .style("fill", function(d) { return color(d.name); });

  var tooltip = d3.select("body")
    .append('div')
    .attr('class', 'tooltip');

    tooltip.append('div')
    .attr('class', 'month');

    tooltip.append('div')
    .attr('class', 'tempRange');


  state.selectAll("rect")
       .on("mouseover", function(d){
          //console.log(d.y1)
          var delta = d.y1 - d.y0;
          var xPos = parseFloat(d3.select(this).attr("x"));
          var yPos = parseFloat(d3.select(this).attr("y"));
          var height = parseFloat(d3.select(this).attr("height"))

          tooltip.select('.month').html("<b>" + d.name + "</b>");

          tooltip.select('.tempRange').html(delta);

        tooltip.style('display', 'block');
        tooltip.style('opacity',2);

         //d3.select(this).attr("stroke","blue").attr("stroke-width",0.8);
         //  console.log(d.name)

          //svg.append("text")
          //  .attr("x", xPos)
          //  .attr("y", yPos)
          //  .attr("font-family", "sans-serif")
          //  .attr("font-size", "12px")
          //  .style("fill", "black")
          //  .attr("class","tooltip")
          //  .text(d.name + ": " + delta);
       })
       .on("mouseout",function(){
          //svg.select(".tooltip").remove();
          //d3.select(this).attr("stroke","pink").attr("stroke-width",0.2);
          tooltip.style('display', 'none');
          tooltip.style('opacity',0);

        })


  var legend = svg.selectAll(".legend")
      .data(color.domain().slice().reverse())
    .enter().append("g")
      //.attr("class", "legend")
      .attr("class", function (d) {
        legendClassArray.push(d.replace(/\s/g, '')); //remove spaces
        return "legend";
      })
      .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

  //reverse order to match order in which bars are stacked
  legendClassArray = legendClassArray.reverse();

  legend.append("rect")
      .attr("x", width - 18)
      .attr("width", 18)
      .attr("height", 18)
      .style("fill", color)
      .attr("id", function (d, i) {
        return "id" + d.replace(/\s/g, '');
      })
      .on("mouseover",function(){

        if (active_link === "0") d3.select(this).style("cursor", "pointer");
        else {
          if (active_link.split("class").pop() === this.id.split("id").pop()) {
            d3.select(this).style("cursor", "pointer");
          } else d3.select(this).style("cursor", "auto");
        }
      })
      .on("click",function(d){

        if (active_link === "0") { //nothing selected, turn on this selection
          d3.select(this)
            .style("stroke", "black")
            .style("stroke-width", 2);

            active_link = this.id.split("id").pop();
            plotSingle(this);

            //gray out the others
            for (i = 0; i < legendClassArray.length; i++) {
              if (legendClassArray[i] != active_link) {
                d3.select("#id" + legendClassArray[i])
                  .style("opacity", 0.5);
              }
            }

        } else { //deactivate
          if (active_link === this.id.split("id").pop()) {//active square selected; turn it OFF
            d3.select(this)
              .style("stroke", "none");

            active_link = "0"; //reset

            //restore remaining boxes to normal opacity
            for (i = 0; i < legendClassArray.length; i++) {
                d3.select("#id" + legendClassArray[i])
                  .style("opacity", 1);
            }

            //restore plot to original
            restorePlot(d);

          }

        } //end active_link check


      });

  //if(state_code == 'all_states'):
  //   x_text_size = "0.25em"

  legend.append("text")
      .attr("x", width - 24)
      .attr("y", 9)
      .attr("dy", "0.35em")
      .style("text-anchor", "end")
      .text(function(d) { return d; });

  function restorePlot(d) {

    state.selectAll("rect").forEach(function (d, i) {
      //restore shifted bars to original posn
      d3.select(d[idx])
        .transition()
        .duration(1000)
        .attr("y", y_orig[i]);
    })

    //restore opacity of erased bars
    for (i = 0; i < legendClassArray.length; i++) {
      if (legendClassArray[i] != class_keep) {
        d3.selectAll(".class" + legendClassArray[i])
          .transition()
          .duration(1000)
          .delay(750)
          .style("opacity", 1);
      }
    }

  }

  function plotSingle(d) {

    class_keep = d.id.split("id").pop();
    idx = legendClassArray.indexOf(class_keep);

    //erase all but selected bars by setting opacity to 0
    for (i = 0; i < legendClassArray.length; i++) {
      if (legendClassArray[i] != class_keep) {
        d3.selectAll(".class" + legendClassArray[i])
          .transition()
          .duration(1000)
          .style("opacity", 0);
      }
    }

    //lower the bars to start on x-axis
    y_orig = [];
    state.selectAll("rect").forEach(function (d, i) {

      //get height and y posn of base bar and selected bar
      h_keep = d3.select(d[idx]).attr("height");
      y_keep = d3.select(d[idx]).attr("y");
      //store y_base in array to restore plot
      y_orig.push(y_keep);

      h_base = d3.select(d[0]).attr("height");
      y_base = d3.select(d[0]).attr("y");

      h_shift = h_keep - h_base;
      y_new = y_base - h_shift;

      //reposition selected bars
      d3.select(d[idx])
        .transition()
        .ease("bounce")
        .duration(1000)
        .delay(750)
        .attr("y", y_new);

    })

  }

    d3.selectAll(".m")
				.on("click", function() {
					var date = this.getAttribute("value");

					var csv_data;
					if(date == "Jaipur"){
						csv_data = "data/jaipur_Jul-Dec-2018_lab_func.csv";
						var state_code = 'Jaipur'
					}else if(date == "Warangal"){
						csv_data = "data/warangal_Jul-Dec-2018_lab_func.csv";
						var state_code = 'Warangal'
					}else if(date == "Karimnagar"){
						csv_data = "data/karimnagar_Jul-Dec-2018_lab_func.csv";
						var state_code = 'Karimnagar'
					}else if(date == "Rangareddy"){
						csv_data = "data/rangareddy_Jul-Dec-2018_lab_func.csv";
						var state_code = 'Rangareddy'
					}else{
					//All states top 50 schools
						csv_data = "data/sirohi_Jul-Dec-2018_lab_func.csv";
						var state_code = 'Sirohi'
					}
    d3.csv(csv_data, function(error, data) {
  if (error) throw error;
  color.domain(d3.keys(data[0]).filter(function(key) { return key !== "school_server_code"; }));

  data.forEach(function(d) {
    var mystate = d.school_server_code; //add to stock code
    var y0 = 0;
    //d.ages = color.domain().map(function(name) { return {name: name, y0: y0, y1: y0 += +d[name]}; });
    d.ages = color.domain().map(function(name) { return {mystate:mystate, name: name, y0: y0, y1: y0 += +d[name]}; });
    d.total = d.ages[d.ages.length - 1].y1;

  });

  data.sort(function(a, b) { return b.total - a.total; });

  x.domain(data.map(function(d) { return d.school_server_code; }));
  y.domain([0, d3.max(data, function(d) { return d.total; })]);

  var maxWidth = 0
  svg.selectAll("*").remove()

  svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
      .selectAll("text")
         .each(function () {
		        var boxWidth = this.getBBox().width;
		        if (boxWidth > maxWidth) maxWidth = boxWidth;
	            })
	     .attr("transform", "translate(0," + (height - maxWidth) + ")")
         .attr("y", 0)
         .attr("x", 9)
         .attr("dy", ".35em")
         .attr("transform", "rotate(70)")
         .style("text-anchor", "start");

  if (state_code == 'All States'){
    svg.selectAll('.x')
       .selectAll('text')
       .remove()
  }


svg.append("text")
      .attr("transform",
            "translate(" + (width/30) + " ," +
                           (height + margin.top + 75) + ")")
      .style("text-anchor", "right")
      .text("*MIT tools are not considered for analysis, some of the math tools and all english tools are not included");

  svg.append('text')
      .attr("x", 390)
     .attr("y",   50)
      .style("text-anchor", "middle")
      .text("Lab Usage in" + ' ' + state_code + ' ' + "(Jul, 2018 - Dec, 2018)")
      .style("font-size", "18px")
      .style("fill", 'black')

  svg.append('text')
      .attr("x", 390)
      .attr("y",   80)
      .style("text-anchor", "middle")
      .text("")
      .style("font-size", "18px")
      .style("fill", 'black')

  svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end");

  svg.append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 0 - margin.left)
      .attr("x",0 - (height / 2))
      .attr("dy", "0.7em")
      .style("text-anchor", "middle")
      .text("Number of Days");

  var state = svg.selectAll(".state")
      .data(data)
    .enter().append("g")
      .attr("class", "g")
      .attr("transform", function(d) { return "translate(" + "0" + ",0)"; });
      //.attr("transform", function(d) { return "translate(" + x(d.State) + ",0)"; })

  state.selectAll("rect")
      .data(function(d) {
        return d.ages;
      })
    .enter().append("rect")
      .attr("width", x.rangeBand())
      .attr("y", function(d) { return y(d.y1); })
      .attr("x",function(d) { //add to stock code
          return x(d.mystate)
        })
      .attr("height", function(d) { return y(d.y0) - y(d.y1); })
      .attr("class", function(d) {
        classLabel = d.name.replace(/\s/g, ''); //remove spaces
        return "class" + classLabel;
      })
      .style("fill", function(d) { return color(d.name); });

  var tooltip = d3.select("body")
    .append('div')
    .attr('class', 'tooltip');

    tooltip.append('div')
    .attr('class', 'month');

    tooltip.append('div')
    .attr('class', 'tempRange');


  state.selectAll("rect")
       .on("mouseover", function(d){
          //console.log(d.y1)
          var delta = d.y1 - d.y0;
          var xPos = parseFloat(d3.select(this).attr("x"));
          var yPos = parseFloat(d3.select(this).attr("y"));
          var height = parseFloat(d3.select(this).attr("height"))

          tooltip.select('.month').html("<b>" + d.name + "</b>");

          tooltip.select('.tempRange').html(delta);

        tooltip.style('display', 'block');
        tooltip.style('opacity',2);

         //d3.select(this).attr("stroke","blue").attr("stroke-width",0.8);
         //  console.log(d.name)

          //svg.append("text")
          //  .attr("x", xPos)
          //  .attr("y", yPos)
          //  .attr("font-family", "sans-serif")
          //  .attr("font-size", "12px")
          //  .style("fill", "black")
          //  .attr("class","tooltip")
          //  .text(d.name + ": " + delta);
       })
       .on("mouseout",function(){
          //svg.select(".tooltip").remove();
          //d3.select(this).attr("stroke","pink").attr("stroke-width",0.2);
          tooltip.style('display', 'none');
          tooltip.style('opacity',0);

        })


 // state.selectAll("rect")
 //      .on("mouseover", function(d){

 //         var delta = d.y1 - d.y0;
 //         var xPos = parseFloat(d3.select(this).attr("x"));
 //         var yPos = parseFloat(d3.select(this).attr("y"));
 //         var height = parseFloat(d3.select(this).attr("height"))

 //         d3.select(this).attr("stroke","blue").attr("stroke-width",0.8);

 //         svg.append("text")
 //         .attr("x",xPos)
 //         .attr("y",yPos +height/2)
 //         .attr("class","tooltip")
 //         .text(d.name +": "+ delta);

 //      })
 //      .on("mouseout",function(){
 //         svg.select(".tooltip").remove();
 //         d3.select(this).attr("stroke","pink").attr("stroke-width",0.2);

 //       })

  var legend = svg.selectAll(".legend")
      .data(color.domain().slice().reverse())
    .enter().append("g")
      //.attr("class", "legend")
      .attr("class", function (d) {
        legendClassArray.push(d.replace(/\s/g, '')); //remove spaces
        return "legend";
      })
      .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

  //reverse order to match order in which bars are stacked
  legendClassArray = legendClassArray.reverse();

  legend.append("rect")
      .attr("x", width - 18)
      .attr("width", 18)
      .attr("height", 18)
      .style("fill", color)
      .attr("id", function (d, i) {
        return "id" + d.replace(/\s/g, '');
      })
      .on("mouseover",function(){

        if (active_link === "0") d3.select(this).style("cursor", "pointer");
        else {
          if (active_link.split("class").pop() === this.id.split("id").pop()) {
            d3.select(this).style("cursor", "pointer");
          } else d3.select(this).style("cursor", "auto");
        }
      })
      .on("click",function(d){

        if (active_link === "0") { //nothing selected, turn on this selection
          d3.select(this)
            .style("stroke", "black")
            .style("stroke-width", 2);

            active_link = this.id.split("id").pop();
            plotSingle(this);

            //gray out the others
            for (i = 0; i < legendClassArray.length; i++) {
              if (legendClassArray[i] != active_link) {
                d3.select("#id" + legendClassArray[i])
                  .style("opacity", 0.5);
              }
            }

        } else { //deactivate
          if (active_link === this.id.split("id").pop()) {//active square selected; turn it OFF
            d3.select(this)
              .style("stroke", "none");

            active_link = "0"; //reset

            //restore remaining boxes to normal opacity
            for (i = 0; i < legendClassArray.length; i++) {
                d3.select("#id" + legendClassArray[i])
                  .style("opacity", 1);
            }

            //restore plot to original
            restorePlot(d);

          }

        } //end active_link check


      });

  legend.append("text")
      .attr("x", width - 24)
      .attr("y", 9)
      .attr("dy", ".35em")
      .style("text-anchor", "end")
      .text(function(d) { return d; });

  function restorePlot(d) {

    state.selectAll("rect").forEach(function (d, i) {
      //restore shifted bars to original posn
      d3.select(d[idx])
        .transition()
        .duration(1000)
        .attr("y", y_orig[i]);
    })

    //restore opacity of erased bars
    for (i = 0; i < legendClassArray.length; i++) {
      if (legendClassArray[i] != class_keep) {
        d3.selectAll(".class" + legendClassArray[i])
          .transition()
          .duration(1000)
          .delay(750)
          .style("opacity", 1);
      }
    }

  }

  function plotSingle(d) {

    class_keep = d.id.split("id").pop();
    idx = legendClassArray.indexOf(class_keep);

    //erase all but selected bars by setting opacity to 0
    for (i = 0; i < legendClassArray.length; i++) {
      if (legendClassArray[i] != class_keep) {
        d3.selectAll(".class" + legendClassArray[i])
          .transition()
          .duration(1000)
          .style("opacity", 0);
      }
    }

    //lower the bars to start on x-axis
    y_orig = [];
    state.selectAll("rect").forEach(function (d, i) {

      //get height and y posn of base bar and selected bar
      h_keep = d3.select(d[idx]).attr("height");
      y_keep = d3.select(d[idx]).attr("y");
      //store y_base in array to restore plot
      y_orig.push(y_keep);

      h_base = d3.select(d[0]).attr("height");
      y_base = d3.select(d[0]).attr("y");

      h_shift = h_keep - h_base;
      y_new = y_base - h_shift;

      //reposition selected bars
      d3.select(d[idx])
        .transition()
        .ease("bounce")
        .duration(1000)
        .delay(750)
        .attr("y", y_new);

    })

  }




});

});

});

};