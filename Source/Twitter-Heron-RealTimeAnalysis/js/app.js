 'use strict';

  var app=angular
      .module('tweetanalysis', ['ngRoute','ngMap','chart.js']);



 app.config(function (ChartJsProvider) {
     // Configure all charts
     ChartJsProvider.setOptions({
         chartColors: ['#FF5252', '#FF8A80']
     });
     // Configure all line charts
     ChartJsProvider.setOptions('line', {
         showLines: true
     });
 });

  app.controller('mapController', function($scope,$http, $interval, NgMap) {
      var vm = this;
      vm.dynMarkers = [];
      NgMap.getMap().then(function(map) {

          var labels = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
          var locations2=$http({
              method: 'GET',
              url: 'https://api.mlab.com/api/1/databases/' +
              'twitterdata' + '/collections/' + 'analyticdata' +
              '?apiKey=' + 'dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf'
          }).success(function (data) {
              //console.log(data);
              if (data) {
                  //console.log('geolocation: ' + data[0].locations);
                  $scope.coordinatesarray = data[0].locations;
                  locations2=data[0].locations;

                  //console.log('inside function: '+data[0].locations[0].lat);
                  var markers = locations2.map(function(location, i) {
                      return new google.maps.Marker({
                          position: location,
                          label: labels[i % labels.length]
                      });
                  });

                  /*
                   for (var i=0; i<1000; i++) {
                   var latLng = new google.maps.LatLng(markers[i].position[0], markers[i].position[1]);
                   vm.dynMarkers.push(new google.maps.Marker({position:latLng}));
                   }*/
                  vm.markerClusterer = new MarkerClusterer(map, markers, {imagePath:'images/m'});


              }
          })
          //console.log(JSON.stringify(locations2));
          //console.log(JSON.stringify(locations2.$$state.valueOf()));
          //console.log(locations2);
          //console.dir(locations2);

      });
  });



  app.controller('locations', function ($scope,$http,$interval) {
    $scope.message="Hello world from angular";
      console.log("manikanta");
      var loc;

      $scope.reload = function () {
          $http({
              method: 'GET',
              url: 'https://api.mlab.com/api/1/databases/' +
              'twitterdata' + '/collections/' + 'analyticdata' +
              '?apiKey=' + 'dm5bCtmORnMEZi6vIWBcUrtfWXnbjtWf'
          }).success(function (data) {
              //console.log(data);
              if (data) {
                  //console.log('geolocation: ' + data[0].locations);
                  $scope.coordinatesarray = data[0].locations;
                  loc=data[0].locations;

                  //console.log('inside function: '+data[0].locations[0].lat);
                  updateScope();
              }
          })

      }
      $scope.reload();
      $interval($scope.reload, 1000);

      var updateScope = function() {
          console.log(loc);
          // your code here
      };
  });

  app.controller("sourceController", function ($scope, $http,$interval) {


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

              $scope.labels = ["Web", "Android","iPhone","Blackberry","Windows Phone"];
              $scope.series = ['Series A'];
              $scope.data = [[data[0].Web_Count,data[0].Android_Count,data[0].iPhone_Count,data[0].Blackberry_Count,data[0].Windows_Count]];

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


      $scope.update();
      $interval($scope.update, 1000);
  });

