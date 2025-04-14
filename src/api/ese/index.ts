import request from '@/utils/request';

export const getElevation = async () => request.get('/ecoessess/elevation');

export const getSlope = async () => request.get('/ecoessess/slope');

export const getAspect = async () => request.get('/ecoessess/aspect');

export const getWaterArea = async () => request.get('/ecoessess/waterArea');

export const getLanduse = async () => request.get('/ecoessess/landuse');

export const getVegetation = async () => request.get('/ecoessess/vegetation');

export const getComprehensive = async () => request.get('/ecoessess/comprehensive');
