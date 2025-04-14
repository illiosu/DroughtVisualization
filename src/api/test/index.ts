import request from '@/utils/request';
enum API {
  comBaseApi = '/sensitivity/com/',
  demBaseApi = '/sensitivity/dem/',
  landuseBaseApi = '/sensitivity/landuse/',
  slopeBaseApi = '/sensitivity/slope/',
  vegBaseApi = '/sensitivity/veg/',
  waterBaseApi = '/sensitivity/water/',
  aspectBaseApi = '/sensitivity/aspect/',
  landTypeBaseApi = '/landtype/',
  getShp = '/njshps/getShp/',
  basePointApi = '/warnpoint/'
}
export const PieOptionApi = async (layername: any) => {
  if (layername === '综合评价结果') {
    return request.get<any, any>(API.comBaseApi + 'getAreaProportion');
    // return await request.get('/sensitivity/com');
  }
  if (layername === '重分类坡向') {
    return request.get<any, any>(API.aspectBaseApi + 'getAreaProportion');
  }
  if (layername === '重分类坡度') {
    return request.get<any, any>(API.slopeBaseApi + 'getAreaProportion');
  }
  if (layername === '重分类植被覆盖度') {
    return request.get<any, any>(API.vegBaseApi + 'getAreaProportion');
  }
  if (layername === '重分类水域缓冲区') {
    return request.get<any, any>(API.waterBaseApi + 'getAreaProportion');
  }
  if (layername === '重分类高程') {
    return request.get<any, any>(API.demBaseApi + 'getAreaProportion');
  }
  if (layername === '重分类地类') {
    return request.get<any, any>(API.landuseBaseApi + 'getAreaProportion');
  }
};
export const PanelOptionApi = async (layername: any) => {
  if (layername === '综合评价结果') {
    return request.get<any, any>(API.comBaseApi + 'getAveRank');
    // return await request.get('/sensitivity/com');
  }
  if (layername === '重分类坡向') {
    return request.get<any, any>(API.aspectBaseApi + 'getAveRank');
  }
  if (layername === '重分类坡度') {
    return request.get<any, any>(API.slopeBaseApi + 'getAveRank');
  }
  if (layername === '重分类植被覆盖度') {
    return request.get<any, any>(API.vegBaseApi + 'getAveRank');
  }
  if (layername === '重分类水域缓冲区') {
    return request.get<any, any>(API.waterBaseApi + 'getAveRank');
  }
  if (layername === '重分类高程') {
    return request.get<any, any>(API.demBaseApi + 'getAveRank');
  }
  if (layername === '重分类地类') {
    return request.get<any, any>(API.landuseBaseApi + 'getAveRank');
  }
};
export const BarOptionApi = async (layername: any) => {
  if (layername === '综合评价结果') {
    return request.get<any, any>(API.comBaseApi + 'getAreas');
    // return await request.get('/sensitivity/com');
  }
  if (layername === '重分类坡向') {
    return request.get<any, any>(API.aspectBaseApi + 'getAreas');
  }
  if (layername === '重分类坡度') {
    return request.get<any, any>(API.slopeBaseApi + 'getAreas');
  }
  if (layername === '重分类植被覆盖度') {
    return request.get<any, any>(API.vegBaseApi + 'getAreas');
  }
  if (layername === '重分类水域缓冲区') {
    return request.get<any, any>(API.waterBaseApi + 'getAreas');
  }
  if (layername === '重分类高程') {
    return request.get<any, any>(API.demBaseApi + 'getAreas');
  }
  if (layername === '重分类地类') {
    return request.get<any, any>(API.landuseBaseApi + 'getAreas');
  }
  if (layername === '2000') {
    return request.get<any, any>(API.landTypeBaseApi + 'get2000');
  }
  if (layername === '2010') {
    return request.get<any, any>(API.landTypeBaseApi + 'get2010');
  }
  if (layername === '2020') {
    return request.get<any, any>(API.landTypeBaseApi + 'get2020');
  }
};
export const ShpApi = async (layername: any) => {
  return request.get<any, any>(API.getShp + `?name=${layername}`);
};
export const addPointApi = async (data: any) => {

  if (data.id) {
    return request.post<any, any>(API.basePointApi  +'addpoint/'+`${data.id}`, data);
  } else {
    return request.post<any, any>(API.basePointApi +'addpoint/',data );
}};
export const getPointsApi = async () => {
  return request.get<any, any>(API.basePointApi +'getPoints/' );
};
export const reqGetPoints = async (page: number, limit: number) => {
  
  return request.get<any, any>(API.basePointApi +'getPointsLimit/'+ `?page=${page}&limit=${limit}` );
};
export const reqGetPointsCount = async () => {
  return request.get<any, any>(API.basePointApi +'getPointsCount/');
};
export const reqDelPoint = async (id: number) => {
  return request.post<any, any>(API.basePointApi +'delpoint/' + `${id}`);
};
// export const reqAddorUpdateScenicSpot = (data: any) => {
//   if (data.id) {
//     return request.post<any, any>(API.addScenicSpot + `${data.id}`, data);
//   } else {
//     return request.post<any, any>(API.addScenicSpot, data);
//   }
// };
