<template>
    <div id="leftPane" class="column">
        <h1>Annotator</h1>
        <!-- <button @click="increment">Count is: {{ count }}</button> -->
    </div>
</template>



<script setup>
import { ref, onMounted } from 'vue'

// reactive state
const count = ref(0)

// functions that mutate state and trigger updates
function increment() {
    count.value++
}

// lifecycle hooks
onMounted(() => {
    console.log(`The initial count is ${count.value}.`)
})
</script>



<style>
#leftPane {
    background-color: #414E5A;
    /* bottom: 0; */
    color: white;
    /* overflow: hidden; */
    /* position: absolute; */

    /* top: 40px; */
    /* left: 0; */
    /* width: 200px; */
    /* height: 1000px; */
    height: 100vh;
    /* height: 100%; */
}
</style>
