import { Routes } from '@angular/router';
import { SearchComponent } from './features/search/search.component';

export const routes: Routes = [
    { path: 'login', loadComponent: () => import('./features/auth/login/login.component').then(m => m.LoginComponent) },
    { path: 'signup', loadComponent: () => import('./features/auth/signup/signup.component').then(m => m.SignupComponent) },
    { path: '', component: SearchComponent },
    { path: '**', redirectTo: '' }
];
