<template>
  <head>
    <link href="https://webfonts.brand.ucsb.edu/webfont.min.css" rel="stylesheet">
  </head>
  <div class="row">
    <!-- <h2>Hello World</h2> -->
    <LeftPane />
    <RightPane />
  </div>
</template>


<script setup>
name: 'TheApp'
import LeftPane from "@/components/2Panes/LeftPane.vue";
import RightPane from "@/components/2Panes/RightPane.vue";


const title = getTitle(this)
if (title) {
  document.title = title
}

function getTitle(vm) {
  return "The Annotator"
}
</script>


<style>

.row {
	  display: grid;
	  grid-template-columns: 200px 1fr;
	}
	.column {
	  padding: 16px;
	  height: 250px;
	}


/* override browser default */
html,
body {
  margin: 0;
  padding: 0;
  /* font-family: serif; */
  font-family: "Avenir Next", Avenir, "Century Gothic", sans-serif;
}

/* use viewport-relative units to cover page fully */


#appContainer {
  background-color: lightcoral;
  top: 0px;
  left: 0px;
  width: 100vw;
  height: 100vh;
}


/* include border and padding in element width and height */

/* full-sized  container that fills up the page */
/* div { */
/* height: 100%; */
/* width: 100%; */
/* width: 100vw; */

/* example padding, font-size, background, etc */
/* padding: 10px; */
/* font-size: 20px; */
/* background-color: lightskyblue; */
/* } */
</style>