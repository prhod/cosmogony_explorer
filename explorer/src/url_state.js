import { initMap } from './map'

/* init default parameters */
export const State = {
  center : [0,50],
  zoom : 4,
  hierarchyId : null
}
const urlHash = window.location.hash

if (window.location.hash.split('/').length > 4) {
  State.hierarchyId = parseInt(window.location.hash.split('/')[4])
}

export function iniUrl() {
  
  if (urlHash && urlHash.split('/').length > 3) { /* parse uri */
    let centerParams = urlHash.replace('#', '').split('/')
    /* remove the # */
    State.center = [parseFloat(centerParams[2]), parseFloat(centerParams[3])]
    State.zoom = parseFloat(centerParams[1])
    initMap(State.center, State.zoom)
  } else { /* no center given : check user location */
    if ('geolocation' in navigator) {
      navigator.geolocation.getCurrentPosition((position) => {
        State.center = [position.coords.longitude, position.coords.latitude]
        initMap(State.center, State.zoom)
      }, () => {
        initMap(State.center, State.zoom)
      })
    }
    else {
      initMap(State.center, State.zoom)
    }
  }
}

export function updateState(o) {
  Object.assign(State, o)
  let urlhash = `#/${State.zoom}/${State.center[0]}/${State.center[1]}${State.hierarchyId ? `/${State.hierarchyId}` : ''}`
  if(history && typeof history.replaceState === 'undefined') {
    location.replace(urlhash)
  } else {
    history.replaceState(null, null, urlhash);
  }

}
export function pushState(o) {
  Object.assign(State, o)
  let urlhash = `#/${State.zoom}/${State.center[0]}/${State.center[1]}${State.hierarchyId ? `/${State.hierarchyId}` : ''}`
  if(history && typeof history.pushState === 'undefined') {
    location.replace(urlhash)
  } else {
    history.pushState(null, null, urlhash);
  }
}

window.onpopstate = function () {
  if(window.location.hash.split('/').length > 3) {
    let tempHierarchyId = parseInt(window.location.hash.split('/')[4])
    /* update only for hierarchy navigation */
    fire('update_hierarchy', tempHierarchyId, {skipPushState : true})
    fire('select_hierarchy', tempHierarchyId)
    let zoom = parseFloat(window.location.hash.split('/')[1])
    let lng = parseFloat(window.location.hash.split('/')[2])
    let lat = parseFloat(window.location.hash.split('/')[3])
    fire('fit_map', zoom, {lng : lng, lat : lat})
  }
}
