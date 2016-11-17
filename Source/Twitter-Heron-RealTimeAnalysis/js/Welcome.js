var app = angular.module("twitter", ['ngRoute','ngMap','chart.js'])


app.controller('mapController', function($scope,$http, $interval, NgMap) {
    var vm = this;
    vm.dynMarkers = [];
    $scope.rell=function(){
    NgMap.getMap().then(function(map) {

        var labels = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
        var locations2=$http({
            method: 'GET',
            url: 'https://api.mlab.com/api/1/databases/' +
            'twitterdata' + '/collections/' + 'analyticdata' +
            '?apiKey=' + 'dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf'
        }).success(function (data) {
            if (data) {
                //console.log('geolocation: ' + data[0].locations);
                $scope.coordinatesarray = data[0].locations;
                locations2=data[0].locations;

                var markers = locations2.map(function(location, i) {
                    return new google.maps.Marker({
                        position: location,
                        label: "",
                        icon:'img/google/measle_red.png'
                    });
                });

                vm.markerClusterer = new MarkerClusterer(map, markers, {imagePath:'img/google/m'});


            }
        })

    });}

    $scope.rell();
    $interval($scope.rell, 1000);
});


app.controller("countController", function ($scope, $http, $interval) {
    var valuer;
    $scope.pageClass = 'count';
        $scope.count = function() {
        $http({
            method: 'GET',
            url : 'https://api.mongolab.com/api/1/databases/rbdlab7/collections//keyframescount?apiKey=dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf',
            data: JSON.stringify({
            }),
            contentType: "application/json"
        }).success(function(data) {
            $scope.Mobile_count = data[0].Mobile_Count;
            $scope.Web_count = data[0].Web_Count;
            $scope.iPhone_count = data[0].iPhone_Count;
            $scope.Android_count = data[0].Android_Count;
            $scope.Blackberry_count = data[0].Blackberry_Count;
            $scope.Windows_count = data[0].Windows_Count;
            $scope.TotalTweet_count = data[0].TotalTweet_Count;

            $scope.msg = "Mobile: "+ $scope.Mobile_count + " Web: "+$scope.Web_count+" iPhone: "+ $scope.iPhone_count +" Android: "+$scope.Android_count+" Blackberry: "+$scope.Blackberry_count+" Windows_Count "+$scope.Windows_count;
        })
    }

    $scope.count();
    $interval($scope.count, 1000);
});

app.controller("chartController", function ($scope, $http, $interval) {


    $scope.count = function() {
        $http({
            method: 'GET',
            url : 'https://api.mongolab.com/api/1/databases/rbdlab7/collections//keyframescount?apiKey=dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf',
            data: JSON.stringify({
            }),
            contentType: "application/json"
        }).success(function(data) {
            console.log("xxxxxxxxxxx"+data);
            $scope.Mobile_count = data[0].Mobile_Count;
            $scope.Web_count = data[0].Web_Count;
            $scope.iPhone_count = data[0].iPhone_Count;
            $scope.Android_count = data[0].Android_Count;
            $scope.Blackberry_count = data[0].Blackberry_Count;
            $scope.Windows_count = data[0].Windows_Count;
            $scope.TotalTweet_count = data[0].TotalTweet_Count;

            $scope.msg = "Mobile: "+ $scope.Mobile_count + " Web: "+$scope.Web_count+" iPhone: "+ $scope.iPhone_count +" Android: "+$scope.Android_count+" Blackberry: "+$scope.Blackberry_count+" Windows_Count "+$scope.Windows_count;

            $scope.labels = ["Android", "Blackberry","iPhone","Web","Windows"];
            $scope.series = ['Series A'];
            $scope.data = [[data[0].Android_Count,data[0].Blackberry_Count,data[0].iPhone_Count,data[0].Web_Count,data[0].Windows_Count]];

            $scope.options = {
                scales: {
                    yAxes: [
                        {
                            id: 'y-axis-1',
                            type: 'linear',
                            display: true,
                            position: 'left'
            }]}};
        })
    }

    $scope.count();
    $interval($scope.count, 1000);
});



app.controller("hashTagController", function ($scope, $http,$interval) {
    var i = 0, j = 0;

    $scope.hashTag = function () {
        $http({
            method: 'GET',
            url: 'https://api.mlab.com/api/1/databases/' +
            'calmcoders' + '/collections/' + 'twitterdata' +
            '?apiKey=' + 'fLr-gBAV6_SS8Qtf-0-MEBlfR51d2nQr'
        }).success(function (data) {
            console.log(data[0]);
            if (data) {
                console.log("data:" + data[0]);
                $scope.One = data[0].One.split(",");
                $scope.Two = data[0].Two.split(",");
                $scope.Three = data[0].Three.split(",");
                $scope.Four = data[0].Four.split(",");
                $scope.Five = data[0].Five.split(",");
                $scope.Six = data[0].Six.split(",");
                $scope.Seven = data[0].Seven.split(",");
                $scope.Eight = data[0].Eight.split(",");
                $scope.Nine = data[0].Nine.split(",");
                $scope.Ten = data[0].Ten.split(",");
            }
        })
    }
    $scope.sentimentchart = function() {

        $http({
            method: 'GET',
            url: 'https://api.mlab.com/api/1/databases/' +
            'calmcoders' + '/collections/' + 'twitterdata' +
            '?apiKey=' + 'fLr-gBAV6_SS8Qtf-0-MEBlfR51d2nQr'
        }).success(function (data) {
            console.log("hello")
            var positive = data[1].Positive;
            var negative = data[1].Negative;
            var neutral = data[1].Neutral;
            google.charts.load('current', {'packages':['corechart']});
            google.charts.setOnLoadCallback(function () {
                drawChart(positive,negative,neutral);
            });
        })
    }

    function drawChart(positive,negative,neutral) {

        var data = google.visualization.arrayToDataTable([
            ['Type', 'Value'],
            ['Positive', positive],
            ['Negative', negative],
            ['Neutral', neutral]
        ]);

        var options = {

        };

        var chart = new google.visualization.PieChart(document.getElementById('piechart'));

        chart.draw(data, options);
    }
    $scope.hashTag();
    $scope.sentimentchart();
    $interval($scope.hashTag, 1000);
    $interval($scope.sentimentchart, 1000);
});

app.controller("barchart", function ($scope, $http,$interval) {

    console.log("1");
    $scope.update2 = function() {
        //console.log("inside login function");
        $http({
            method: 'GET',
            url: 'https://api.mlab.com/api/1/databases/' +
            'twitterdata' + '/collections/' + 'countriesdata' +
            '?apiKey=' + 'dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf'

        }).success(function(data) {
            console.log("top country data"+data);
            console.log('country1: ' + data[0].topcountries[0].country);
            $scope.coordinatesarray = data[0].topcountries;
            var countriesarray=data[0].topcountries;
            var countryname=data[0].topcountries[0].country;


            $scope.labels3 = [data[0].topcountries[0].country, data[0].topcountries[1].country,data[0].topcountries[2].country];
            $scope.series3 = ['Series A'];
            $scope.colors3=["#000000","#FFFFFF"];

            $scope.data3 = [
                [data[0].topcountries[0].count,data[0].topcountries[1].count,data[0].topcountries[2].count]
            ];


        })
    }


    $scope.update2();
    $interval($scope.update2, 1000);
});

app.controller("BubbleCtrl", function($scope,$http, $interval, NgMap) {

    $scope.update = function() {
        //console.log("inside login function");
        $http({
            method: 'GET',
            url : 'https://api.mongolab.com/api/1/databases/rbdlab7/collections//keyframescount?apiKey=dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf',
            data: JSON.stringify({
            }),
            contentType: "application/json"
        }).success(function(data) {
                //console.log(data);
                //console.log(data[0].Android_Count);
                $scope.Mobile_count = data[0].Mobile_Count;
                $scope.Web_count = data[0].Web_Count;
                $scope.iPhone_count = data[0].iPhone_Count;
                $scope.Android_count = data[0].Android_Count;
                $scope.Blackberry_count = data[0].Blackberry_Count;
                $scope.Windows_count = data[0].Windows_Count;
                $scope.TotalTweet_count = data[0].TotalTweet_Count;

                $scope.msg = "Mobile: "+ $scope.Mobile_count + " Web: "+$scope.Web_count+" iPhone: "+ $scope.iPhone_count +" Android: "+$scope.Android_count+" Blackberry: "+$scope.Blackberry_count+" Windows_Count "+$scope.Windows_count;

                var x=data[0].TotalTweet_Count;
                //console.log(x);
                $scope.mobile ="Mobile Count";

                $scope.labels2=['webcount','mobilecount'];
                $scope.labelString2=['xwwxx','yyyy'];
                $scope.options2 = {
                    legend: {display:true,textStyle:{fontSize:200}},
                    scales: {
                        xAxes: [{
                            display: false,
                            ticks: {
                                max: data[0].TotalTweet_Count*3,
                                min: -data[0].TotalTweet_Count*3,
                                stepSize: data[0].TotalTweet_Count
                            }
                        }],
                        yAxes: [{
                            display: false,
                            ticks: {
                                max: (data[0].TotalTweet_Count)*3,
                                min: -(data[0].TotalTweet_Count)*3,
                                stepSize: data[0].TotalTweet_Count
                            }
                        }]
                    }
                };
                // see examples/bubble.js for random bubbles source code
                $scope.series2 = ['Web Count', 'Mobile Count']
                $scope.colors2=["#000000","#FF6384"]

                $scope.data2 = [
                    [{
                        x: -data[0].TotalTweet_Count,
                        y: 0,
                        r: (data[0].Web_Count)%100,
                        label: "Chaitu"
                    }],
                    [{
                        x: 0,
                        y: (data[0].TotalTweet_Count),
                        r: (data[0].Mobile_Count)%100
                    }]]
            }
        )}
    $scope.update();
    $interval($scope.update, 3000);
});

